import os
import json
import smtplib
import yfinance as yf
import google.generativeai as genai
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import time
import re
import random
from email.mime.base import MIMEBase
from email import encoders
import pandas_ta as ta

# ----------------------------------------------------------------------
# 1. CONFIGURACIÓN
# ----------------------------------------------------------------------

# --- Google Sheets API Config ---
# RUTA AL ARCHIVO DE CREDENCIALES
SERVICE_ACCOUNT_FILE = 'service_account.json' 
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
# ID DE LA HOJA DE CÁLCULO
# ¡RECUERDA REEMPLAZAR ESTE ID CON TU ID REAL!
SPREADSHEET_ID = '1234567890abcdefghijklmnopqrstuvwxyz' 

# --- Diccionario de Tickers (Ejemplo para IBEX) ---
# Se asume que este diccionario es el completo de tu análisis (80+ empresas)
tickers = {
    'Acciona': 'ANA.MC',
    'Accionarenovables': 'ANE.MC',
    'Acerinox': 'ACX.MC',
    'ACS': 'ACS.MC',
    'Aedas-Homes': 'AEDAS.MC',
    'Aena': 'AENA.MC',
    'Almirall': 'ALM.MC',
    'Airbus': 'AIR.MC',
    'AirTificial': 'AI.MC',
    'Amadeus': 'AMS.MC',
    'Amper': 'AMP.MC',
    'Audax-Renovables': 'ADX.MC',
    'Bankinter': 'BKT.MC',
    'BBVA': 'BBVA.MC',
    'Berkeley': 'BKY.MC',
    'Biotechnology': 'BST.MC',
    'CaixaBank': 'CABK.MC',
    'Cellnex': 'CLNX.MC',
    'DIA': 'DIA.MC',
    'Ercros': 'ECR.MC',
    'Endesa': 'ELE.MC',
    'Elecnor': 'ENO.MC',
    # ... (Añadir el resto de tus 80+ tickers aquí)
}

# ----------------------------------------------------------------------
# 2. FUNCIONES DE UTILIDAD
# ----------------------------------------------------------------------

def formatear_numero(numero, decimales=2):
    """Formatea un número a cadena con separador de miles y decimales."""
    try:
        if isinstance(numero, (int, float, np.number)):
            return f"{numero:,.{decimales}f}".replace(",", "_TEMP_").replace(".", ",").replace("_TEMP_", ".")
        return str(numero)
    except Exception:
        return str(numero)

def obtener_datos_yfinance(ticker):
    """Obtiene los datos históricos de YFinance y calcula indicadores."""
    try:
        data = yf.download(ticker, period="3mo", interval="1d", progress=False)
        if data.empty:
            return None
        
        # Calcular indicadores
        data.ta.sma(length=20, append=True)
        data.ta.ema(length=100, append=True)
        data.ta.rsi(length=14, append=True)
        data.ta.macd(append=True)
        
        # Obtener el último precio
        precio_actual = data['Close'].iloc[-1]
        
        # Obtener valores de indicadores
        ema_100 = data['EMA_100'].iloc[-1]
        rsi_14 = data['RSI_14'].iloc[-1]
        
        # --- Lógica de Soportes/Resistencias (Simplificada) ---
        # Usamos Low/High de 30 y 60 días como referencias
        low_30 = data['Low'].tail(30).min()
        high_30 = data['High'].tail(30).max()
        low_60 = data['Low'].tail(60).min()
        high_60 = data['High'].tail(60).max()
        
        # Definición de Soportes/Resistencias
        soporte_1 = min(low_30, precio_actual * 0.95)
        soporte_2 = low_60
        resistencia_1 = max(high_30, precio_actual * 1.05)
        resistencia_2 = high_60

        # --- Análisis Semanal SMI (Simulado) ---
        # Obtener datos semanales para simular SMI
        weekly_data = yf.download(ticker, period="1y", interval="1wk", progress=False)
        if not weekly_data.empty:
            weekly_data.ta.stoch(k=14, d=3, smooth_k=3, append=True)
            stoch_k = weekly_data['STOCHk_14_3_3'].iloc[-1]
            stoch_d = weekly_data['STOCHd_14_3_3'].iloc[-1]
            
            if stoch_k < 20 and stoch_d < 20:
                observacion_semanal = "El estocástico semanal (SMI) indica una zona de fuerte sobreventa, potencial rebote."
            elif stoch_k > 80 and stoch_d > 80:
                observacion_semanal = "El estocástico semanal (SMI) indica una zona de fuerte sobrecompra, potencial corrección."
            else:
                observacion_semanal = "El estocástico semanal (SMI) se encuentra en zona neutral."
        else:
            observacion_semanal = "No se pudo obtener el análisis semanal."

        return {
            'TICKER': ticker,
            'PRECIO_ACTUAL': precio_actual,
            'VALOR_EMA': ema_100,
            'RSI_14': rsi_14,
            'SOPORTE_1': soporte_1,
            'SOPORTE_2': soporte_2,
            'RESISTENCIA_1': resistencia_1,
            'RESISTENCIA_2': resistencia_2,
            'OBSERVACION_SEMANAL': observacion_semanal,
            'DATA': data # Datos completos para análisis posterior
        }
    except Exception as e:
        # print(f"Error al obtener datos de YFinance para {ticker}: {e}")
        return None

def clasificar_empresa(data):
    """Clasifica la empresa según su estado técnico."""
    
    precio = data['PRECIO_ACTUAL']
    ema = data['VALOR_EMA']
    rsi = data['RSI_14']
    data_df = data['DATA']
    nombre = next((n for n, t in tickers.items() if t == data['TICKER']), data['TICKER'])
    
    # --- 1. Tendencia ---
    tendencia = "Lateral"
    tipo_ema = "Por debajo"
    if precio > ema:
        tendencia = "Alcista"
        tipo_ema = "Por encima"
    elif precio < ema * 0.95: # Bajista si está 5% por debajo
        tendencia = "Bajista"
    
    # --- 2. Oportunidad y Niveles ---
    oportunidad = "VIGILAR - Consolidación"
    compra_si = ""
    vende_si = ""
    orden_grupo = 7 # Por defecto, sin movimientos
    
    # MACD Cruce alcista (último valor)
    macd_val = data_df['MACD_12_26_9'].iloc[-1]
    macds_val = data_df['MACDh_12_26_9'].iloc[-1]
    macd_cruce_alcista = (macd_val > macds_val) and (data_df['MACDh_12_26_9'].iloc[-2] < 0) and (data_df['MACDh_12_26_9'].iloc[-1] > 0)
    
    # Condición de Compra (Basado en EMA y RSI)
    if precio > ema and rsi < 70 and macd_cruce_alcista:
        oportunidad = "COMPRA FUERTE"
        compra_si = formatear_numero(precio) + "€ (Actual)"
        vende_si = formatear_numero(data['SOPORTE_1']) + "€ (Stop)"
        orden_grupo = 1
    
    # Condición de Compra de Riesgo (Bajo EMA, pero sobreventa)
    elif precio < ema and rsi < 30 and (precio < data['SOPORTE_1']):
        oportunidad = "COMPRA DE RIESGO - Sobreventa"
        compra_si = formatear_numero(precio) + "€ (Actual)"
        vende_si = formatear_numero(data['SOPORTE_2']) + "€ (Stop)"
        orden_grupo = 2.5
    
    # Condición de Venta/Riesgo (Sobrecompra)
    elif rsi > 70 and precio > data['RESISTENCIA_1']:
        oportunidad = "VENTA/CORRECCIÓN - Sobrecompra"
        compra_si = formatear_numero(data['RESISTENCIA_2']) + "€ (Largo)"
        vende_si = formatear_numero(precio) + "€ (Actual)"
        orden_grupo = 3

    # Condición de Vigilancia (Tendencia estable)
    elif tendencia == "Alcista" and rsi < 60:
        oportunidad = "COMPRA VIGILAR - Consolidación"
        compra_si = formatear_numero(data['SOPORTE_1']) + "€"
        vende_si = formatear_numero(data['RESISTENCIA_1']) + "€"
        orden_grupo = 2
    
    # Condición de Tendencia Bajista/Lateral (RSI Neutral)
    elif tendencia in ["Bajista", "Lateral"] and 30 <= rsi <= 70:
        oportunidad = "VIGILAR - Sin señal clara"
        compra_si = formatear_numero(data['RESISTENCIA_1']) + "€"
        vende_si = formatear_numero(data['SOPORTE_1']) + "€"
        orden_grupo = 4

    # --- 3. Devolver el diccionario completo ---
    return {
        'NOMBRE_EMPRESA': nombre,
        'TICKER': data['TICKER'],
        'PRECIO_ACTUAL': data['PRECIO_ACTUAL'],
        'TENDENCIA_ACTUAL': tendencia,
        'TIPO_EMA': tipo_ema,
        'VALOR_EMA': ema,
        'RSI_14': rsi,
        'OPORTUNIDAD': oportunidad,
        'COMPRA_SI': compra_si,
        'VENDE_SI': vende_si,
        'SOPORTE_1': data['SOPORTE_1'],
        'SOPORTE_2': data['SOPORTE_2'],
        'RESISTENCIA_1': data['RESISTENCIA_1'],
        'RESISTENCIA_2': data['RESISTENCIA_2'],
        'OBSERVACION_SEMANAL': data['OBSERVACION_SEMANAL'],
        'ORDEN_GRUPO': orden_grupo # Clave para ordenar
    }

def obtener_clave_ordenacion(data):
    """Define la clave de ordenación para la tabla HTML."""
    # Ordenar por el grupo principal (1=Compra Fuerte, 7=Sin Movimientos)
    # y luego por RSI de menor a mayor (más cerca de sobreventa, más interesante)
    return (data['ORDEN_GRUPO'], data['RSI_14'])

def generar_observaciones(data):
    """Genera las observaciones detalladas del algoritmo."""
    observaciones = [
        f"**EMA (100 días):** El precio está **{data['TIPO_EMA']}** de la EMA de 100 en {formatear_numero(data['VALOR_EMA'])}€.",
        f"**RSI (14 días):** Indica {formatear_numero(data['RSI_14'])}.",
        f"**Soportes:** Niveles clave en S1: {formatear_numero(data['SOPORTE_1'])}€ y S2: {formatear_numero(data['SOPORTE_2'])}€.",
        f"**Resistencias:** Niveles clave en R1: {formatear_numero(data['RESISTENCIA_1'])}€ y R2: {formatear_numero(data['RESISTENCIA_2'])}€."
    ]
    return "<br>".join(observaciones)

# ----------------------------------------------------------------------
# 3. GOOGLE SHEETS
# ----------------------------------------------------------------------

def leer_google_sheets():
    """Lee la lista de usuarios premium con sus planes y empresas."""
    try:
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        service = build('sheets', 'v4', credentials=creds)

        # Nombre de la hoja y rango 
        range_name = 'Usuarios!A2:D999' 
        
        # Llama a la API de Sheets
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range=range_name).execute()
        
        values = result.get('values', [])

        if not values:
            print('No se encontraron datos de usuarios premium.')
            return []
        
        print(f"Se encontraron {len(values)} usuarios premium para procesar.")
        
        # Filtra y limpia los datos: Nombre, Email, Plan, Empresas
        usuarios_limpios = []
        for row in values:
            if len(row) >= 4:
                usuarios_limpios.append([row[0].strip(), row[1].strip(), row[2].strip(), row[3].strip()])
            else:
                 # Añadir campos vacíos si faltan para evitar el error de índice
                while len(row) < 4:
                    row.append('')
                usuarios_limpios.append([row[0].strip(), row[1].strip(), row[2].strip(), row[3].strip()])

        return usuarios_limpios
    
    except Exception as e:
        print(f"❌ Error al leer Google Sheets: {e}")
        return []

# ----------------------------------------------------------------------
# 4. FUNCIÓN MODIFICADA: ENVÍO DE CORREO (Con saludo, alineación izquierda y fecha)
# ----------------------------------------------------------------------
def enviar_email(html_content, asunto_email, destinatario_usuario, nombre_usuario, fecha_asunto, hora_asunto):
    """Envía el correo al destinatario especificado con el HTML en el cuerpo."""
    
    # REMITENTE VISIBLE: EL QUE VERÁ EL USUARIO
    remitente_visible = "info@ibexia.es" 
    
    # LOGIN REAL: EL CORREO GMAIL ASOCIADO A LA CLAVE DE APLICACIÓN
    remitente_login = "xumkox@gmail.com"
    password = "kdgz lvdo wqvt vfkt" 

    # 1. Crear el Saludo y el Cuerpo Completo del Mensaje (¡ALINEADO A LA IZQUIERDA!)
    saludo_profesional = f"""
    <div style="text-align: left; max-width: 100%;">
        <p style="font-size: 1.1em; color: #000; margin-bottom: 20px;">
            **Estimado/a {nombre_usuario},**
        </p>
        <p style="font-size: 1em; color: #000; margin-bottom: 25px;">
            Nos complace presentarte tu **Reporte Premium de Oportunidades Bursátiles** de IBEXIA, correspondiente al **{fecha_asunto} a las {hora_asunto} horas**.
            Este análisis exclusivo se basa en la aplicación rigurosa de nuestro algoritmo para las empresas seleccionadas de tu plan. 
            Te invitamos a revisar los niveles de oportunidad, soporte y resistencia en la tabla detallada a continuación.
        </p>
        <p style="font-size: 0.9em; color: #000; margin-top: 15px;">
            Para cualquier consulta o duda sobre tu análisis, no dudes en contactar con nuestro equipo de soporte en <a href="mailto:info@ibexia.es" style="color: #007bff; text-decoration: none;">**info@ibexia.es**</a>.
        </p>
    </div>
    """
    
    # Se inserta el saludo antes del contenido principal (la tabla HTML)
    cuerpo_final_html = saludo_profesional + html_content

    msg = MIMEMultipart('alternative')
    msg['From'] = remitente_visible
    msg['To'] = destinatario_usuario 
    msg['Subject'] = asunto_email

    # Adjuntar el HTML como cuerpo del mensaje
    part = MIMEText(cuerpo_final_html, 'html')
    msg.attach(part)

    try:
        servidor = smtplib.SMTP('smtp.gmail.com', 587)
        servidor.starttls()
        servidor.login(remitente_login, password) 
        servidor.sendmail(remitente_visible, destinatario_usuario, msg.as_string()) 
        print(f"✅ Correo enviado a {destinatario_usuario} desde {remitente_visible} con el asunto: {asunto_email}")
        servidor.quit()
        
    except Exception as e:
        print(f"❌ Error al enviar el correo a {destinatario_usuario} desde {remitente_visible}: {e}")

# ----------------------------------------------------------------------
# 5. FUNCIÓN MODIFICADA: GENERAR FILA DE REPORTE (Detalles visibles)
# ----------------------------------------------------------------------
def generar_fila_reporte(data):
    """Genera la fila principal, la fila de detalle y la fila de observaciones para una empresa, mostrando el detalle por defecto."""
    
    # Lógica para determinar el enlace (se mantiene)
    global tickers
    nombre_empresa_url = None
    for nombre, ticker_val in tickers.items():
        if ticker_val == data['TICKER']:
            nombre_empresa_url = nombre
            break
    
    if nombre_empresa_url:
        empresa_link = f'https://ibexia.es/category/{nombre_empresa_url.lower()}/'
    else:
        empresa_link = '#'
    
    nombre_con_precio = f"<a href='{empresa_link}' target='_blank' style='text-decoration:none; color:inherit;'><div class='stacked-text'><b>{data['NOMBRE_EMPRESA']}</b><br>({formatear_numero(data['PRECIO_ACTUAL'])}€)</div></a>"

    if "compra" in data['OPORTUNIDAD'].lower() and "riesgo" not in data['OPORTUNIDAD'].lower():
        clase_oportunidad = "compra"
        celda_empresa_class = "green-cell"
    elif "venta" in data['OPORTUNIDAD'].lower():
        clase_oportunidad = "venta"
        celda_empresa_class = "red-cell"
    elif "vigilar" in data['OPORTUNIDAD'].lower():
        clase_oportunidad = "vigilar"
        celda_empresa_class = ""
    elif "riesgo" in data['OPORTUNIDAD'].lower():
        clase_oportunidad = "riesgo-compra"
        celda_empresa_class = "yellow-cell"
    else:
        clase_oportunidad = ""
        celda_empresa_class = ""
    
    
    observaciones = generar_observaciones(data)
    
    # --- FILAS DE REPORTE AHORA VISIBLES POR DEFECTO Y COLSPAN 5 ---
    return f"""
                <tr class="main-row">
                    <td class="{celda_empresa_class}">{nombre_con_precio}</td>
                    <td>{data['TENDENCIA_ACTUAL']}</td>
                    <td class="{clase_oportunidad}">{data['OPORTUNIDAD']}</td>
                    <td>{data['COMPRA_SI']}</td>
                    <td>{data['VENDE_SI']}</td>
                </tr>
                <tr class="detailed-row-static">
                    <td colspan="5">
                        <div style="display:flex; justify-content:space-around; align-items:flex-start; padding: 10px; font-size: 0.9em;">
                            <div style="flex-basis: 20%; text-align:left;">
                                <b>EMA (100)</b><br>
                                <span style="font-weight:bold;">{formatear_numero(data['VALOR_EMA'])}€</span><br>
                                ({data['TIPO_EMA']})
                            </div>
                            <div style="flex-basis: 20%; text-align:left;">
                                <b>Soportes</b><br>
                                S1: {formatear_numero(data['SOPORTE_1'])}€<br>
                                S2: {formatear_numero(data['SOPORTE_2'])}€
                            </div>
                            <div style="flex-basis: 20%; text-align:left;">
                                <b>Resistencias</b><br>
                                R1: {formatear_numero(data['RESISTENCIA_1'])}€<br>
                                R2: {formatear_numero(data['RESISTENCIA_2'])}€
                            </div>
                            <div style="flex-basis: 40%; text-align:left; font-size:0.9em;">
                                <b>Análisis Semanal (SMI)</b><br>
                                {data['OBSERVACION_SEMANAL']}
                            </div>
                        </div>
                    </td>
                </tr>
                <tr class="observaciones-row">
                    <td colspan="5">{observaciones}</td>
                </tr>
    """

# ----------------------------------------------------------------------
# 6. FUNCIÓN MODIFICADA: GENERAR HTML REPORTE (Sin JavaScript y Colspan 5)
# ----------------------------------------------------------------------
def generar_html_reporte(datos_ordenados, nombre_usuario):
    """Genera el contenido HTML del reporte."""
    
    now_utc = datetime.utcnow()
    # Usar la misma lógica de tiempo para el título interno
    time_offset = timedelta(hours=2) # Asumo CEST/CET +2 horas
    local_time = now_utc + time_offset
    hora_actual = local_time.strftime('%H:%M')

    html_body = f"""
        <html>
        <head>
            <title>Resumen Diario de Oportunidades - {datetime.today().strftime('%d/%m/%Y')} {hora_actual}</title>
            <style>
                body {{
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                    background-color: #f8f9fa;
                    margin: 0;
                    padding: 10px;
                }}
                .main-container {{
                    max-width: 1200px;
                    margin: 0 auto;
                    background-color: #ffffff;
                    padding: 15px;
                    border-radius: 8px;
                    box-shadow: 0 2px 5px rgba(0, 0, 0, 0.05);
                }}
                h2 {{
                    color: #343a40;
                    text-align: center;
                    font-size: 1.5em;
                    margin-bottom: 10px;
                }}
                p {{
                    color: #6c757d;
                    text-align: center;
                    font-size: 0.9em;
                }}
                .table-container {{
                    overflow-x: auto;
                    overflow-y: auto;
                    max-height: 70vh; 
                    position: relative;
                }}
                table {{
                    width: 100%;
                    margin: 10px auto 0 auto;
                    border-collapse: collapse;
                    font-size: 0.85em;
                    min-width: 600px;
                }}
                th, td {{
                    border: 1px solid #e9ecef;
                    padding: 6px;
                    text-align: center;
                    vertical-align: middle;
                    white-space: normal;
                    line-height: 1.2;
                }}
                th {{
                    background-color: #e9ecef;
                    color: #495057;
                    font-weight: 600;
                    position: sticky;
                    top: 0;
                    z-index: 10;
                    white-space: nowrap;
                }}
                .compra {{ color: #28a745; font-weight: bold; }}
                .venta {{ color: #dc3545; font-weight: bold; }}
                .riesgo-compra {{ color: #ffc107; font-weight: bold; }} 
                .comprado-si {{ background-color: #28a745; color: white; font-weight: bold; }}
                .bg-green {{ background-color: #d4edda; color: #155724; }}
                .bg-red {{ background-color: #f8d7da; color: #721c24; }}
                .bg-highlight {{ background-color: #28a745; color: white; font-weight: bold; }}
                .text-center {{ text-align: center; }}
                .disclaimer {{ font-size: 0.8em; text-align: center; color: #6c757d; margin-top: 15px; }}
                .small-text {{ font-size: 0.7em; color: #6c757d; }}
                .green-cell {{ background-color: #d4edda; }}
                .red-cell {{ background-color: #f8d7da; }}
                .yellow-cell {{ background-color: #fff3cd; }} 
                .separator-row td {{ background-color: #e9ecef; height: 3px; padding: 0; border: none; }}
                .category-header td {{
                    background-color: #495057;
                    color: white;
                    font-size: 1.1em;
                    font-weight: bold;
                    text-align: center;
                    padding: 10px;
                    border: none;
                }}
                .observaciones-row td {{
                    background-color: #f9f9f9;
                    text-align: left;
                    font-size: 0.8em;
                    border: 1px solid #e9ecef;
                }}
                .stacked-text {{
                    line-height: 1.2;
                    font-size: 0.8em;
                }}
                .vigilar {{ color: #ffc107; font-weight: bold; }}
                
                /* Las filas detalladas ahora son visibles por defecto sin JavaScript */
                .collapsible-row, .expand-button {{
                    display: none; 
                }}
            </style>
        </head>
        <body>
            <div class="main-container">
                <h2 class="text-center">Resumen Diario de Oportunidades ordenadas por prioridad - {datetime.today().strftime('%d/%m/%Y')} {hora_actual}</h2>
                
                <div class="table-container">
                    <table id="myTable">
                        <thead>
                            <tr>
                                <th>Empresa (Precio)</th>
                                <th>Tendencia Actual</th>
                                <th>Oportunidad</th>
                                <th>Compra si...</th>
                                <th>Vende si...</th>
                            </tr>
                        </thead>
                        <tbody>
        """
        
    # --- ZONA CORREGIDA DEL ERROR DE INDENTACIÓN (Línea 551) ---
    if not datos_ordenados:
        html_body += """
                        <tr><td colspan="5">No se encontraron empresas con datos válidos hoy.</td></tr>
        """
    else:
        previous_orden_grupo = None
        for i, data in enumerate(datos_ordenados):
            
            current_orden_grupo = obtener_clave_ordenacion(data)[0]
            
            # Lógica para determinar el encabezado de categoría
            es_primera_fila = previous_orden_grupo is None
            es_cambio_grupo = current_orden_grupo != previous_orden_grupo
            
            if es_primera_fila or es_cambio_grupo:
                
                # Colspan corregido a 5
                if current_orden_grupo in [1, 2, 2.5]: 
                    if previous_orden_grupo is None or previous_orden_grupo not in [1, 2, 2.5]:
                        html_body += """
                            <tr class="category-header"><td colspan="5">OPORTUNIDADES DE COMPRA</td></tr>
                        """
                elif current_orden_grupo in [3, 4, 5]:
                    if previous_orden_grupo is None or previous_orden_grupo not in [3, 4, 5]:
                        html_body += """
                            <tr class="category-header"><td colspan="5">ATENTOS A VENDER/VIGILANCIA</td></tr>
                        """
                elif current_orden_grupo in [6, 7]:
                    if previous_orden_grupo is None or previous_orden_grupo not in [6, 7]:
                        html_body += """
                            <tr class="category-header"><td colspan="5">OTRAS EMPRESAS SIN MOVIMIENTOS</td></tr>
                        """
                        
                # Colspan corregido a 5
                if not es_primera_fila and es_cambio_grupo:
                    html_body += """
                        <tr class="separator-row"><td colspan="5"></td></tr>
                    """
            
            html_body += generar_fila_reporte(data) 
            
            previous_orden_grupo = current_orden_grupo
    
    html_body += """
                    </tbody>
                </table>
            </div>
            
            <br>
            <p class="disclaimer"><strong>Aviso:</strong> El algoritmo de trading se basa en indicadores técnicos y no garantiza la rentabilidad. Utiliza esta información con tu propio análisis y criterio. ¡Feliz trading!</p>
        </div>
        
        </body>
    </html>
    """
    return html_body

# ----------------------------------------------------------------------
# 7. FUNCIÓN PRINCIPAL (Generar Reporte y Bucle de Envío)
# ----------------------------------------------------------------------
def generar_reporte():
    try:
        # CÁLCULO DE FECHA Y HORA (SE HACE UNA VEZ AL PRINCIPIO)
        now_utc = datetime.utcnow()
        time_offset = timedelta(hours=2) # Asumo CEST/CET +2 horas
        local_time = now_utc + time_offset
        fecha_asunto = local_time.strftime('%d/%m')
        hora_asunto = local_time.strftime('%H:%M')

        # 1. ANÁLISIS GLOBAL: Procesar TODAS las 80+ empresas una sola vez
        print("Iniciando análisis global de todas las empresas...")
        datos_completos_por_ticker = {}
        
        # Iterar sobre una copia de los tickers para evitar problemas si se modifica el diccionario
        for empresa_nombre, ticker in list(tickers.items()): 
            try:
                data = obtener_datos_yfinance(ticker)
                if data:
                    datos_completos_por_ticker[ticker] = clasificar_empresa(data)
            except Exception as e:
                print(f"❌ Error al procesar {ticker} en el análisis global: {e}")

        # 2. PROCESAR USUARIOS Y ENVIAR PERSONALIZADO
        print("\nIniciando envíos personalizados a usuarios premium...")
        usuarios_premium = leer_google_sheets()

        for usuario in usuarios_premium:
            try:
                # Desestructurar los 4 campos esperados
                if len(usuario) < 4:
                    print(f"⚠️ Fila de usuario incompleta: {usuario}. Saltando...")
                    continue
                    
                nombre_usuario, email_usuario, plan_usuario, empresas_str = usuario
                
                print(f"\n⚙️ Procesando usuario: {nombre_usuario} ({email_usuario}) - Plan: {plan_usuario}")
                
                # 3. DETERMINAR LOS TICKERS ESPECÍFICOS PARA ESTE USUARIO
                plan_limpio = plan_usuario.upper().strip()
                
                if plan_limpio == 'LOTE':
                    # Si es LOTE, usa todos los datos analizados
                    datos_para_reporte = list(datos_completos_por_ticker.values())
                else:
                    # Convertir la cadena de empresas a una lista de tickers válidos
                    nombres_elegidos = [n.strip() for n in empresas_str.split(',')]
                    tickers_del_usuario = [
                        tickers[nombre_largo] 
                        for nombre_largo in nombres_elegidos 
                        if nombre_largo in tickers
                    ]
                    
                    # Filtrar los datos analizados previamente (paso 1)
                    datos_para_reporte = [
                        datos_completos_por_ticker[t] 
                        for t in tickers_del_usuario 
                        if t in datos_completos_por_ticker
                    ]
                
                if not datos_para_reporte:
                    print(f"⚠️ Usuario {nombre_usuario} no tiene empresas válidas o no se encontraron datos. Saltando envío...")
                    continue
                    
                # 4. ORDENAR DATOS Y GENERAR HTML PERSONALIZADO
                datos_ordenados = sorted(datos_para_reporte, key=obtener_clave_ordenacion)

                # Generar el HTML personalizado
                html_body = generar_html_reporte(datos_ordenados, nombre_usuario)

                # 5. ENVIAR CORREO PERSONALIZADO
                # ASUNTO CON EL FORMATO REQUERIDO: "ANALISIS PREMIUM DD/MM HH:MM horas."
                asunto = f"ANALISIS PREMIUM {fecha_asunto} {hora_asunto} horas."
                
                # Llamada a la función con los nuevos argumentos de fecha y hora
                enviar_email(html_body, asunto, email_usuario, nombre_usuario, fecha_asunto, hora_asunto) 

            except Exception as e:
                print(f"❌ Error al procesar el usuario {usuario}: {e}")

        print("\nProceso de envío de correos premium completado.")

    except Exception as e:
        print(f"❌ Error al ejecutar el script principal: {e}")

if __name__ == '__main__':
    generar_reporte()
