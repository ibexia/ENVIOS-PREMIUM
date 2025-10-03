import os
import json
import smtplib
import yfinance as yf
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
# DICCIONARIO DE TICKERS
# ----------------------------------------------------------------------
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
    'ENCE': 'ENC.MC',
    'Enagas': 'ENG.MC',
    'Ezentis': 'EZE.MC',
    'FacePhi': 'FACE.MC',
    'Ferrovial': 'FER.MC',
    'Fomento Construcciones y Contratas': 'FCC.MC',
    'Fluidra': 'FDR.MC',
    'GAM': 'GAM.MC',
    'Gigas-Hosting': 'GIGA.MC',
    'Grifols': 'GRF.MC',
    'Grupo San Jose': 'GSJ.MC',
    'Holaluz': 'HLZ.MC',
    'Neinor-homes': 'HOME.MC',
    'IAG': 'IAG.MC',
    'Iberdrola': 'IBE.MC',
    'Iberpapel': 'IBG.MC',
    'Inditex': 'ITX.MC',
    'Indra': 'IDR.MC',
    'Logista': 'LOG.MC',
    'Linea-directa': 'LDA.MC',
    'Mapfre': 'MAP.MC',
    'duro-felguera': 'MDF.MC',
    'melia': 'MEL.MC',
    'Merlin': 'MRL.MC',
    'arcelor-mittal': 'MTS.MC',
    'Naturgy': 'NTGY.MC',
    'nbi-bearings': 'NBI.MC',
    'nextil': 'NXT.MC',
    'nyesa': 'NYE.MC',
    'ohla': 'OHLA.MC',
    'Deoleo': 'OLE.MC',
    'Oryzon': 'ORY.MC',
    'Pharma-Mar': 'PHM.MC',
    'Prosegur': 'PSG.MC',
    'Puig-brands': 'PUIG.MC',
    'Red-Electrica': 'RED.MC',
    'Repsol': 'REP.MC',
    'Laboratorios-rovi': 'ROVI.MC',
    'Banco-sabadell': 'SAB.MC',
    'Sacyr': 'SCYR.MC',
    'Solaria': 'SLR.MC',
    'Squirrel': 'SQRL.MC',
    'Substrate': 'SAI.MC',
    'banco-santander': 'SAN.MC',
    'Talgo': 'TLGO.MC',
    'Telefonica': 'TEF.MC',
    'Tubos-Reunidos': 'TRG.MC',
    'tubacex': 'TUB.MC',
    'Unicaja': 'UNI.MC',
    'Viscofan': 'VIS.MC',
    'Urbas': 'URB.MC',
}

# ----------------------------------------------------------------------
# 1. FUNCIÓN MODIFICADA: LECTURA DE GOOGLE SHEETS
# Se modifica para leer las 4 columnas (Nombre, Email, Plan, Empresas).
# ----------------------------------------------------------------------
def leer_google_sheets():
    """Lee la lista de usuarios, sus planes y empresas elegidas."""
    try:
        credentials_json = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        if not credentials_json:
            raise Exception("No se encontró la variable de entorno GOOGLE_APPLICATION_CREDENTIALS")

        creds_dict = json.loads(credentials_json)
        creds = service_account.Credentials.from_service_account_info(
            creds_dict,
            scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
        )
    except Exception as e:
        print(f"Error en credenciales: {e}")
        return []

    spreadsheet_id = os.getenv('SPREADSHEET_ID')
    if not spreadsheet_id:
        raise Exception("No se encontró la variable de entorno SPREADSHEET_ID")
    
    # Rango: A: Nombre, B: Email, C: Plan, D: Empresas
    # Asumo Hoja1, empezando en Fila 2 (A2) para saltar el encabezado
    range_name = 'Hoja 1!A2:D' 

    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
    values = result.get('values', [])

    if not values:
        print('No se encontraron usuarios premium.')
    else:
        print(f'Se encontraron {len(values)} usuarios para procesar.')
        
    # Devuelve: [['Nombre', 'Email', 'Plan', 'Empresas'], ...]
    return values 

# ----------------------------------------------------------------------
# FUNCIONES AUXILIARES (SE MANTIENEN IGUAL)
# ----------------------------------------------------------------------
def formatear_numero(numero):
    if pd.isna(numero) or numero is None:
        return "N/A"
    try:
        num = float(numero)
        return f"{num:,.3f}"
    except (ValueError, TypeError):
        return "N/A"
        
def calculate_smi_tv(df):
    high = df['High']
    low = df['Low']
    close = df['Close']
    length_k = 10
    length_d = 3
    ema_signal_len = 10
    smooth_period = 5
    hh = high.rolling(window=length_k).max()
    ll = low.rolling(window=length_k).min()
    diff = hh - ll
    rdiff = close - (hh + ll) / 2
    avgrel = rdiff.ewm(span=length_d, adjust=False).mean()
    avgdiff = diff.ewm(span=length_d, adjust=False).mean()
    epsilon = 1e-9
    smi_raw = np.where(
        (avgdiff / 2 + epsilon) != 0,
        (avgrel / (avgdiff / 2 + epsilon)) * 100,
        0.0
    )
    smi_raw = np.clip(smi_raw, -100, 100)
    smi_smoothed = pd.Series(smi_raw, index=df.index).rolling(window=smooth_period).mean()
    smi_signal = smi_smoothed.ewm(span=ema_signal_len, adjust=False).mean()
    df['SMI'] = smi_smoothed
    return df

def calcular_precio_aplanamiento(df):
    try:
        if len(df) < 3:
            return "N/A"

        length_d = 3
        smooth_period = 5

        df_prev = df.iloc[:-1].copy()
        df_prev = calculate_smi_tv(df_prev)

        avgrel_prev_last = (df_prev['Close'] - (df_prev['High'].rolling(window=10).max() + df_prev['Low'].rolling(window=10).min()) / 2).ewm(span=length_d, adjust=False).mean().iloc[-1]
        avgdiff_prev_last = (df_prev['High'].rolling(window=10).max() - df_prev['Low'].rolling(window=10).min()).ewm(span=length_d, adjust=False).mean().iloc[-1]
        smi_raw_yesterday = df['SMI'].iloc[-2]

        alpha_ema = 2 / (length_d + 1)
        
        hh_today = df['High'].rolling(window=10).max().iloc[-1]
        ll_today = df['Low'].rolling(window=10).min().iloc[-1]
        diff_today = hh_today - ll_today
        
        avgdiff_today = (1 - alpha_ema) * avgdiff_prev_last + alpha_ema * diff_today
        
        avgrel_today_target = (smi_raw_yesterday / 100) * (avgdiff_today / 2)
        
        rdiff_today_target = (avgrel_today_target - (1 - alpha_ema) * avgrel_prev_last) / alpha_ema
        
        close_target = rdiff_today_target + (hh_today + ll_today) / 2
        
        return close_target

    except Exception as e:
        print(f"❌ Error en el cálculo de precio de aplanamiento: {e}")
        return "N/A"

def calcular_soporte_resistencia(df, window=5):
    try:
        supports = []
        resistances = []
        
        if len(df) < window * 2:
            return {'s1': 'N/A', 's2': 'N/A', 'r1': 'N/A', 'r2': 'N/A'}

        for i in range(window, len(df) - window):
            high_slice = df['High'].iloc[i - window : i + window + 1]
            low_slice = df['Low'].iloc[i - window : i + window + 1]

            if df['High'].iloc[i] == high_slice.max():
                resistances.append(df['High'].iloc[i])
            
            if df['Low'].iloc[i] == low_slice.min():
                supports.append(df['Low'].iloc[i])

        supports = sorted(list(set(supports)), reverse=True)
        resistances = sorted(list(set(resistances)))
        
        current_price = df['Close'].iloc[-1]
        
        s1 = next((s for s in supports if s < current_price), None)
        s2 = next((s for s in supports if s < current_price and s != s1), None)
        
        r1 = next((r for r in resistances if r > current_price), None)
        r2 = next((r for r in resistances if r > current_price and r != r1), None)

        return {'s1': s1, 's2': s2, 'r1': r1, 'r2': r2}
        
    except Exception as e:
        print(f"❌ Error al calcular soportes y resistencias: {e}")
        return {'s1': 'N/A', 's2': 'N/A', 'r1': 'N/A', 'r2': 'N/A'}
        
def calcular_beneficio_perdida(precio_compra, precio_actual, inversion=10000):
    try:
        precio_compra = float(precio_compra)
        precio_actual = float(precio_actual)
        
        if precio_compra <= 0 or precio_actual <= 0:
            return "N/A"

        acciones = inversion / precio_compra
        beneficio_perdida = (precio_actual - precio_compra) * acciones
        return f"{beneficio_perdida:,.2f}"
    except (ValueError, TypeError):
        return "N/A"

def obtener_datos_yfinance(ticker):
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        
        current_price = info.get("currentPrice")
        if not current_price:
            print(f"⚠️ Advertencia: No se encontró precio actual para {ticker}. Saltando...")
            return None

        # --- Datos Diarios (como estaban) ---
        hist_extended = stock.history(period="150d", interval="1d")
        hist_extended['EMA_100'] = ta.ema(hist_extended['Close'], length=100)
                
        precio_actual = hist_extended['Close'].iloc[-1]
        ema_actual = hist_extended['EMA_100'].iloc[-1]
        
        if precio_actual > ema_actual:
            tipo_ema = "Soporte"
        elif precio_actual < ema_actual:
            tipo_ema = "Resistencia"
        else:
            tipo_ema = "Igual"
            
        if hist_extended.empty:
            print(f"⚠️ Advertencia: No se encontraron datos históricos para {ticker}. Saltando...")
            return None
        hist_extended = calculate_smi_tv(hist_extended)
        
        sr_levels = calcular_soporte_resistencia(hist_extended)

        smi_series = hist_extended['SMI'].dropna()
        if len(smi_series) < 2:
            print(f"⚠️ Advertencia: No hay suficientes datos de SMI para {ticker}. Saltando...")
            return None
        
        smi_yesterday = smi_series.iloc[-2]
        smi_today = smi_series.iloc[-1]
        
        pendiente_hoy = smi_today - smi_yesterday
        
        tendencia_hoy = "Subiendo" if pendiente_hoy > 0.1 else ("Bajando" if pendiente_hoy < -0.1 else "Plano")
        
        estado_smi = "Sobrecompra" if smi_today > 40 else ("Sobreventa" if smi_today < -40 else "Intermedio")
        
        precio_aplanamiento = calcular_precio_aplanamiento(hist_extended)
        
        comprado_status = "NO"
        precio_compra = "N/A"
        fecha_compra = "N/A"
        
        smi_series_copy = hist_extended['SMI'].copy()
        pendientes_smi = smi_series_copy.diff()
        
        for i in range(len(hist_extended) - 1, 0, -1):
            smi_prev = hist_extended['SMI'].iloc[i - 1]
            pendiente_prev = pendientes_smi.iloc[i - 1]
            pendiente_curr = pendientes_smi.iloc[i]
            
            if pendiente_curr < 0 and pendiente_prev >= 0:
                comprado_status = "NO"
                precio_compra = hist_extended['Close'].iloc[i]
                fecha_compra = hist_extended.index[i].strftime('%d/%m/%Y')
                break
            
            elif pendiente_curr > 0 and pendiente_prev <= 0 and smi_prev < 40:
                comprado_status = "SI"
                precio_compra = hist_extended['Close'].iloc[i]
                fecha_compra = hist_extended.index[i].strftime('%d/%m/%Y')
                break

        # --- Modificación: Cálculo de SMI Semanal ---
        hist_weekly = stock.history(period="3y", interval="1wk")
        if hist_weekly.empty:
            smi_weekly = 'N/A'
            estado_smi_weekly = 'N/A'
            # Nuevo campo para el texto de la observación semanal
            observacion_semanal = "No hay datos semanales suficientes."
        else:
            hist_weekly = calculate_smi_tv(hist_weekly)
            smi_weekly_series = hist_weekly['SMI'].dropna()
            smi_weekly = smi_weekly_series.iloc[-1] if not smi_weekly_series.empty else 'N/A'
            
            if isinstance(smi_weekly, (int, float)):
                estado_smi_weekly = "Sobrecompra" if smi_weekly > 40 else ("Sobreventa" if smi_weekly < -40 else "Intermedio")
                
                # Generar el texto de la observación semanal
                if estado_smi_weekly == "Sobrecompra":
                    observacion_semanal = f"El **SMI Semanal** ({formatear_numero(smi_weekly)}) está en zona de **Sobrecompra**. Sugiere que el precio ya ha subido mucho a largo plazo."
                elif estado_smi_weekly == "Sobreventa":
                    observacion_semanal = f"El **SMI Semanal** ({formatear_numero(smi_weekly)}) está en zona de **Sobreventa**. Sugiere potencial de subida a largo plazo."
                else:
                    observacion_semanal = f"El **SMI Semanal** ({formatear_numero(smi_weekly)}) está en zona **Intermedia**."
                    
            else:
                estado_smi_weekly = 'N/A'
                observacion_semanal = "No hay datos semanales suficientes."


        return {
            "TICKER": ticker,
            "NOMBRE_EMPRESA": info.get("longName", ticker),
            "PRECIO_ACTUAL": current_price,
            "SMI_AYER": smi_yesterday,
            "SMI_HOY": smi_today,
            "TENDENCIA_ACTUAL": tendencia_hoy,
            "ESTADO_SMI": estado_smi,
            "PRECIO_APLANAMIENTO": precio_aplanamiento,
            "PENDIENTE": pendiente_hoy,
            "COMPRADO": comprado_status,
            "PRECIO_COMPRA": precio_compra,
            "FECHA_COMPRA": fecha_compra,
            "HIST_DF": hist_extended,
            "SOPORTE_1": sr_levels['s1'],
            "SOPORTE_2": sr_levels['s2'],
            "RESISTENCIA_1": sr_levels['r1'],
            "TIPO_EMA": tipo_ema,
            "VALOR_EMA": ema_actual,
            "RESISTENCIA_2": sr_levels['r2'],
            # --- Nuevos Campos Semanales ---
            "SMI_SEMANAL": smi_weekly,
            "ESTADO_SMI_SEMANAL": estado_smi_weekly,
            "ADVERTENCIA_SEMANAL": "NO", # Se inicializa y se modifica en clasificar_empresa
            "OBSERVACION_SEMANAL": observacion_semanal # Nuevo campo con el texto de la observación semanal
        }

    except Exception as e:
        print(f"❌ Error al obtener datos de {ticker}: {e}. Saltando a la siguiente empresa...")
        return None

def clasificar_empresa(data):
    estado_smi = data['ESTADO_SMI']
    tendencia = data['TENDENCIA_ACTUAL']
    precio_aplanamiento = data['PRECIO_APLANAMIENTO']
    smi_actual = data['SMI_HOY']
    smi_ayer = data['SMI_AYER']
    hist_df = data['HIST_DF']
    
    current_price = data['PRECIO_ACTUAL']
    close_yesterday = hist_df['Close'].iloc[-2] if len(hist_df) > 1 else 'N/A'

    high_today = hist_df['High'].iloc[-1]
    low_today = hist_df['Low'].iloc[-1]
    
    pendiente_smi_hoy = data['PENDIENTE']
    pendiente_smi_ayer = hist_df['SMI'].diff().iloc[-2] if len(hist_df['SMI']) > 1 else 'N/A'
    
    # --- Nuevo: Variables Semanales ---
    estado_smi_weekly = data['ESTADO_SMI_SEMANAL']

    prioridad = {
        "Posibilidad de Compra Activada": 1,
        "Posibilidad de Compra": 2,
        "VIGILAR": 3,
        "Riesgo de Venta": 4,
        "Riesgo de Venta Activada": 5,
        "Seguirá bajando": 6,
        "Intermedio": 7,
        "Compra RIESGO": 8 
    }

    if estado_smi == "Sobreventa":
        if tendencia == "Subiendo":
            # --- Lógica de Filtro Semanal ---
            if estado_smi_weekly == "Sobrecompra":
                data['OPORTUNIDAD'] = "Compra RIESGO"
                data['COMPRA_SI'] = "NO RECOMENDAMOS" 
                data['VENDE_SI'] = "NO VENDER"
                data['ORDEN_PRIORIDAD'] = prioridad["Compra RIESGO"]
                data['ADVERTENCIA_SEMANAL'] = "SI"
            else:
                data['OPORTUNIDAD'] = "Posibilidad de Compra Activada"
                data['COMPRA_SI'] = "COMPRA YA"
                data['VENDE_SI'] = "NO VENDER"
                data['ORDEN_PRIORIDAD'] = prioridad["Posibilidad de Compra Activada"]
            # -----------------------------------
        elif tendencia == "Bajando":
            # --- Lógica de Filtro Semanal ---
            if estado_smi_weekly == "Sobrecompra":
                data['OPORTUNIDAD'] = "Compra RIESGO"
                data['COMPRA_SI'] = "NO RECOMENDAMOS"
                data['VENDE_SI'] = "NO VENDER"
                data['ORDEN_PRIORIDAD'] = prioridad["Compra RIESGO"]
                data['ADVERTENCIA_SEMANAL'] = "SI"
            else:
                data['OPORTUNIDAD'] = "Posibilidad de Compra"
                if current_price > close_yesterday:
                    data['COMPRA_SI'] = "COMPRA YA"
                else:
                    data['COMPRA_SI'] = f"COMPRAR SI SUPERA {formatear_numero(close_yesterday)}€"
                data['VENDE_SI'] = "NO VENDER"
                data['ORDEN_PRIORIDAD'] = prioridad["Posibilidad de Compra"]
            # -----------------------------------
        else: # Plano
            data['OPORTUNIDAD'] = "Intermedio"
            data['COMPRA_SI'] = "NO PREVEEMOS GIRO EN ESTOS MOMENTOS"
            data['VENDE_SI'] = "NO PREVEEMOS GIRO EN ESTOS MOMENTOS"
            data['ORDEN_PRIORIDAD'] = prioridad["Intermedio"]

    elif estado_smi == "Intermedio":
        if tendencia == "Bajando":
            data['OPORTUNIDAD'] = "Seguirá bajando"
            data['COMPRA_SI'] = "NO COMPRAR"
            data['VENDE_SI'] = "YA ES TARDE PARA VENDER"
            data['ORDEN_PRIORIDAD'] = prioridad["Seguirá bajando"]
        elif tendencia == "Subiendo":
            data['OPORTUNIDAD'] = "VIGILAR"
            data['COMPRA_SI'] = "NO COMPRAR"
            trigger_price = close_yesterday * 0.99
            if current_price < trigger_price:
                data['VENDE_SI'] = "VENDE YA"
            else:
                data['VENDE_SI'] = f"VENDER SI PIERDE {formatear_numero(trigger_price)}€"
            data['ORDEN_PRIORIDAD'] = prioridad["VIGILAR"]
        else: # Plano
            data['OPORTUNIDAD'] = "Intermedio"
            data['COMPRA_SI'] = "NO PREVEEMOS GIRO EN ESTOS MOMENTOS"
            data['VENDE_SI'] = "NO PREVEEMOS GIRO EN ESTOS MOMENTOS"
            data['ORDEN_PRIORIDAD'] = prioridad["Intermedio"]

    elif estado_smi == "Sobrecompra":
        if tendencia == "Subiendo":
            data['OPORTUNIDAD'] = "Riesgo de Venta"
            data['COMPRA_SI'] = "NO COMPRAR"
            data['VENDE_SI'] = f"ZONA DE VENTA<br><span class='small-text'>PRECIO IDEAL VENTA HOY: {high_today:,.2f}€</span>"
            data['ORDEN_PRIORIDAD'] = prioridad["Riesgo de Venta"]
        elif tendencia == "Bajando":
            data['OPORTUNIDAD'] = "Riesgo de Venta Activada"
            data['COMPRA_SI'] = "NO COMPRAR"
            data['VENDE_SI'] = "VENDE AHORA"
            data['ORDEN_PRIORIDAD'] = prioridad["Riesgo de Venta Activada"]
        else: # Plano
            data['OPORTUNIDAD'] = "Intermedio"
            data['COMPRA_SI'] = "NO PREVEEMOS GIRO EN ESTOS MOMENTOS"
            data['VENDE_SI'] = "NO PREVEEMOS GIRO EN ESTOS MOMENTOS"
            data['ORDEN_PRIORIDAD'] = prioridad["Intermedio"]

    return data

# ----------------------------------------------------------------------
# FUNCIONES DE OBSERVACIÓN, HTML Y EMAIL (MODIFICADAS PARA NUEVO FORMATO)
# ----------------------------------------------------------------------
def generar_observaciones(data):
    """Genera el texto de observaciones detalladas para el campo INFO ADICIONAL."""
    nombre_empresa = data['NOMBRE_EMPRESA']
    precio_actual = formatear_numero(data['PRECIO_ACTUAL'])
    smi_actual = formatear_numero(data['SMI_HOY'])
    tendencia = data['TENDENCIA_ACTUAL']
    estado_smi = data['ESTADO_SMI']

    observacion = f"**{nombre_empresa}** ({precio_actual}€)\n\n"
    observacion += f"- **Oportunidad:** {data['OPORTUNIDAD']}\n"
    observacion += f"- **Acción de Compra:** {data['COMPRA_SI'].replace('<br><span class=\'small-text\'>', ' (')}\n" # Limpiar HTML
    observacion += f"- **Acción de Venta:** {data['VENDE_SI'].replace('<br><span class=\'small-text\'>', ' (')}\n\n" # Limpiar HTML

    # Datos Técnicos
    observacion += "**Datos Técnicos Diarios:**\n"
    observacion += f"- SMI Hoy: {smi_actual} (Estado: {estado_smi}, Tendencia: {tendencia})\n"
    observacion += f"- Precio Aplanamiento (objetivo SMI=0): {formatear_numero(data['PRECIO_APLANAMIENTO'])}€\n"
    observacion += f"- Soporte 1 (Corto Plazo): {formatear_numero(data['SOPORTE_1'])}€\n"
    observacion += f"- Resistencia 1 (Corto Plazo): {formatear_numero(data['RESISTENCIA_1'])}€\n"
    
    # EMA Info
    if data['VALOR_EMA'] != 'N/A':
        observacion += f"- EMA-100: {formatear_numero(data['VALOR_EMA'])}€ ({data['TIPO_EMA']})\n"
    
    # Posición de Compra
    if data['COMPRADO'] == "SI":
        observacion += f"\n**HISTORIAL:** Última señal de COMPRA en {formatear_numero(data['PRECIO_COMPRA'])}€ el {data['FECHA_COMPRA']}.\n"
        # Cálculo de Beneficio/Pérdida
        beneficio_perdida = calcular_beneficio_perdida(data['PRECIO_COMPRA'], data['PRECIO_ACTUAL'])
        if beneficio_perdida != 'N/A':
            observacion += f"    - Beneficio/Pérdida estimado (10k€ inv.): {beneficio_perdida}€\n"
    elif data['COMPRADO'] == "NO" and data['PRECIO_COMPRA'] != 'N/A':
        observacion += f"\n**HISTORIAL:** Última señal de VENTA/SALIDA en {formatear_numero(data['PRECIO_COMPRA'])}€ el {data['FECHA_COMPRA']}.\n"
        
    # Advertencia Semanal
    if data['ADVERTENCIA_SEMANAL'] == "SI":
        observacion += f"\n⚠️ **ALERTA SEMANAL:** {data['OBSERVACION_SEMANAL']}"
        
    # Reemplazar **negritas** por <b>negritas</b> y \n por <br> para HTML simple dentro de la celda
    observacion_html = observacion.replace('**', '<b>').replace('<b>', '</b>', 2).replace('\n', '<br>')
    
    data['OBSERVACION_FINAL'] = observacion_html
    return data

def generar_html_reporte(datos_reporte, nombre_usuario):
    """Genera el contenido HTML con el nuevo formato de tabla (filtrado, búsqueda, sticky header)."""

    # --- CSS Styles (para header fijo, filtrado, y look moderno) ---
    css = """
    <style>
        body { font-family: 'Arial', sans-serif; background-color: #f4f4f9; color: #333; margin: 0; padding: 20px; }
        .container { max-width: 1500px; margin: auto; background: #fff; padding: 30px; border-radius: 12px; box-shadow: 0 4px 20px rgba(0, 0, 0, 0.1); }
        h1 { color: #004d99; text-align: center; margin-bottom: 25px; border-bottom: 2px solid #eee; padding-bottom: 15px; }
        
        .search-controls { 
            margin-bottom: 20px; 
            display: flex; 
            gap: 20px; 
            align-items: center; 
            justify-content: center;
            padding: 10px;
            background-color: #f0f0f5;
            border-radius: 8px;
        }
        .search-controls input, .search-controls select { 
            padding: 10px 15px; 
            border: 1px solid #ccc; 
            border-radius: 8px; 
            font-size: 16px; 
            width: 300px; 
        }
        
        /* Contenedor principal de la tabla para scroll y header fijo */
        .table-wrapper { 
            position: relative; 
            max-height: 80vh; 
            overflow-y: auto; 
            overflow-x: auto; 
            border: 1px solid #ddd; 
            border-radius: 8px; 
            box-shadow: 0 2px 5px rgba(0, 0, 0, 0.05);
        }
        
        .table-wrapper table { 
            border-collapse: collapse; 
            width: 100%; 
            min-width: 1400px; /* Asegura un ancho mínimo para scroll horizontal */
        }
        
        /* Header Fijo (Sticky) */
        .table-wrapper th { 
            position: sticky; 
            top: 0; 
            background: #004d99; 
            color: white; 
            padding: 12px 15px; 
            text-align: left; 
            z-index: 10; 
            cursor: pointer; 
            border-right: 1px solid #003366;
        }
        .table-wrapper th:last-child {
            border-right: none;
        }
        .table-wrapper th:hover { background: #003366; }
        
        .table-wrapper td { 
            padding: 12px 15px; 
            border-bottom: 1px solid #eee; 
            vertical-align: top;
        }
        .table-wrapper tr:nth-child(even) { background-color: #f9f9f9; }
        .table-wrapper tr:hover { background-color: #f0f0f0; }
        
        /* Estilos para Badges/Etiquetas */
        .badge { 
            display: inline-block; 
            padding: 5px 10px; 
            border-radius: 20px; 
            font-weight: bold; 
            text-align: center; 
            font-size: 12px; 
            line-height: 1.2;
        }
        .badge-posibilidad-de-compra-activada { background-color: #4CAF50; color: white; } /* Green */
        .badge-posibilidad-de-compra { background-color: #8BC34A; color: white; } /* Light Green */
        .badge-vigilar { background-color: #FFC107; color: #333; } /* Amber */
        .badge-riesgo-de-venta { background-color: #FF9800; color: white; } /* Orange */
        .badge-riesgo-de-venta-activada { background-color: #F44336; color: white; } /* Red */
        .badge-seguirá-bajando { background-color: #9E9E9E; color: white; } /* Gray */
        .badge-intermedio { background-color: #2196F3; color: white; } /* Blue */
        .badge-compra-riesgo { background-color: #E91E63; color: white; } /* Pink/Red Risk */
        .badge-análisis-fallido { background-color: #795548; color: white; } /* Brown */
        
        /* Estilos de Texto */
        .price-up { color: #4CAF50; font-weight: bold; }
        .price-down { color: #F44336; font-weight: bold; }
        .price-flat { color: #333; }
        .small-text { font-size: 0.8em; color: #666; font-weight: normal; }
        .alert-icon { color: red; font-weight: bold; }
    </style>
    """

    # --- JavaScript para Ordenación, Filtrado, Búsqueda y Scroll Fijo ---
    js = """
    <script>
        function getCellValue(tr, idx) {
            // Maneja el contenido HTML dentro de las celdas (ej. en la columna Oportunidad)
            if (idx === 2) {
                const span = tr.children[idx].querySelector('.badge');
                return span ? span.textContent || span.innerText : tr.children[idx].textContent;
            }
            return tr.children[idx].innerText || tr.children[idx].textContent;
        }

        function filterTable() {
            const searchInput = document.getElementById("searchInput").value.toUpperCase();
            const filterSelect = document.getElementById("filterSelect").value.toUpperCase();
            const rows = document.getElementById("reportTableBody").getElementsByTagName("tr");

            for (let i = 0; i < rows.length; i++) {
                const row = rows[i];
                // Columnas a buscar (TICKER: 0, EMPRESA: 1, OPORTUNIDAD: 2)
                const ticker = getCellValue(row, 0).toUpperCase();
                const nombre = getCellValue(row, 1).toUpperCase();
                const oportunidad = getCellValue(row, 2).toUpperCase();

                let searchMatch = (ticker.includes(searchInput) || nombre.includes(searchInput));
                let filterMatch = (filterSelect === "" || oportunidad.includes(filterSelect));

                if (searchMatch && filterMatch) {
                    row.style.display = "";
                } else {
                    row.style.display = "none";
                }
            }
        }
        
        // Función para la ordenación de columnas
        const compareRows = (idx, asc) => (a, b) => {
            const v1 = getCellValue(asc ? a : b, idx).trim();
            const v2 = getCellValue(asc ? b : a, idx).trim();
            
            // Intenta ordenar numéricamente (limpiando € y ,)
            const num1 = parseFloat(v1.replace('€', '').replace(/\./g, '').replace(',', '.'));
            const num2 = parseFloat(v2.replace('€', '').replace(/\./g, '').replace(',', '.'));

            if (!isNaN(num1) && !isNaN(num2) && v1 !== 'N/A' && v2 !== 'N/A') {
                return num1 - num2;
            }

            // Fallback a comparación de strings
            return v1.toString().localeCompare(v2.toString(), 'es', { numeric: true });
        };

        document.querySelectorAll('#reportTable thead th').forEach(header => {
            let asc = true; // Estado inicial: ascendente
            header.addEventListener('click', () => {
                const tbody = document.getElementById('reportTableBody');
                const index = Array.from(header.parentNode.children).indexOf(header);
                
                Array.from(tbody.querySelectorAll('tr'))
                    .sort(compareRows(index, asc))
                    .forEach(tr => tbody.appendChild(tr));
                
                // Toggle sort order for next click
                asc = !asc;
            });
        });
        
    </script>
    """
    
    # --- Table Header ---
    table_headers = [
        "TICKER", "EMPRESA", "OPORTUNIDAD", "COMPRA SI", "VENDE SI",
        "PRECIO ACT. (€)", "SMI HOY", "TENDENCIA", "ESTADO SMI",
        "PRECIO COMPRA", "FECHA COMPRA", "Bº/PÉRDIDA", "SOPORTE 1", "RESISTENCIA 1",
        "INFO ADICIONAL"
    ]
    
    header_row = "".join(f"<th>{h}</th>" for h in table_headers)

    # --- Table Body Rows ---
    table_rows = []
    for data in datos_reporte:
        # 1. Asegurarse de que las observaciones estén generadas para la última columna
        if 'OBSERVACION_FINAL' not in data:
            data = generar_observaciones(data) 
        
        # 2. Clases CSS para los badges/etiquetas
        oportunidad_badge_class = data['OPORTUNIDAD'].lower().replace(' ', '-').replace('/', '-')
        
        # 3. Cálculo de B/P (Beneficio/Pérdida)
        precio_actual_bp = data['PRECIO_ACTUAL']
        # Se asegura que solo calcula B/P si se dio señal de COMPRA y tiene precio
        precio_compra_bp = data['PRECIO_COMPRA'] if data['COMPRADO'] == 'SI' and data['PRECIO_COMPRA'] != 'N/A' else 'N/A'
        beneficio_perdida_str = calcular_beneficio_perdida(precio_compra_bp, precio_actual_bp) if precio_compra_bp != 'N/A' else 'N/A'
        
        
        # 4. Formato de Tendencia
        if data['TENDENCIA_ACTUAL'] == "Subiendo":
            tendencia_text = f"<span class='price-up'>📈 {data['TENDENCIA_ACTUAL']}</span>"
        elif data['TENDENCIA_ACTUAL'] == "Bajando":
            tendencia_text = f"<span class='price-down'>📉 {data['TENDENCIA_ACTUAL']}</span>"
        else:
            tendencia_text = f"<span class='price-flat'>➖ {data['TENDENCIA_ACTUAL']}</span>"
            
        # 5. Formato de Oportunidad (badge)
        oportunidad_text = f"<span class='badge badge-{oportunidad_badge_class}'>{data['OPORTUNIDAD']}</span>"
        
        # 6. Formato de SMI Status
        if data['ESTADO_SMI'] == 'Sobrecompra':
            estado_smi_text = f"<span style='color: #F44336; font-weight: bold;'>⚠️ {data['ESTADO_SMI']}</span>"
        elif data['ESTADO_SMI'] == 'Sobreventa':
            estado_smi_text = f"<span style='color: #4CAF50; font-weight: bold;'>✅ {data['ESTADO_SMI']}</span>"
        else:
            estado_smi_text = data['ESTADO_SMI']
            
        # 7. Formato Compra/Venta Actions (manteniendo el HTML interno para el small-text)
        compra_si_text = data['COMPRA_SI']
        vende_si_text = data['VENDE_SI']
        
        # 8. Añadir un ícono de alerta si aplica el riesgo semanal
        if data['ADVERTENCIA_SEMANAL'] == "SI":
            compra_si_text = f"<span class='alert-icon'>🚨</span> {compra_si_text}"


        # 9. Construir la fila
        row = f"""
        <tr>
            <td>{data['TICKER']}</td>
            <td>{data['NOMBRE_EMPRESA']}</td>
            <td>{oportunidad_text}</td>
            <td>{compra_si_text}</td>
            <td>{vende_si_text}</td>
            <td>{formatear_numero(data['PRECIO_ACTUAL'])}€</td>
            <td>{formatear_numero(data['SMI_HOY'])}</td>
            <td>{tendencia_text}</td>
            <td>{estado_smi_text}</td>
            <td>{formatear_numero(data['PRECIO_COMPRA'])}€</td>
            <td>{data['FECHA_COMPRA']}</td>
            <td>{beneficio_perdida_str}€</td>
            <td>{formatear_numero(data['SOPORTE_1'])}€</td>
            <td>{formatear_numero(data['RESISTENCIA_1'])}€</td>
            <td>{data.get('OBSERVACION_FINAL', 'N/A')}</td>
        </tr>
        """
        table_rows.append(row)

    # --- Estructura HTML Final ---
    html_content = f"""
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Reporte Premium - {nombre_usuario}</title>
        {css}
    </head>
    <body>
        <div class="container">
            <h1>Reporte Premium de Oportunidades - {nombre_usuario}</h1>
            
            <div class="search-controls">
                <input type="text" id="searchInput" placeholder="Buscar por Ticker o Empresa..." onkeyup="filterTable()">
                <select id="filterSelect" onchange="filterTable()">
                    <option value="">Todas las Oportunidades</option>
                    <option value="Posibilidad de Compra Activada">Posibilidad de Compra Activada</option>
                    <option value="Posibilidad de Compra">Posibilidad de Compra</option>
                    <option value="VIGILAR">VIGILAR</option>
                    <option value="Riesgo de Venta">Riesgo de Venta</option>
                    <option value="Riesgo de Venta Activada">Riesgo de Venta Activada</option>
                    <option value="Seguirá bajando">Seguirá bajando</option>
                    <option value="Compra RIESGO">Compra RIESGO</option>
                    <option value="Intermedio">Intermedio</option>
                    <option value="ANÁLISIS FALLIDO">ANÁLISIS FALLIDO</option>
                </select>
            </div>

            <div class="table-wrapper" id="tableContainer">
                <table id="reportTable">
                    <thead>
                        <tr>
                            {header_row}
                        </tr>
                    </thead>
                    <tbody id="reportTableBody">
                        {''.join(table_rows)}
                    </tbody>
                </table>
            </div>

        </div>
        {js}
    </body>
    </html>
    """
    
    return html_content

def obtener_clave_ordenacion(data):
    """Clave de ordenación para priorizar oportunidades de compra y SMI alto."""
    # La ordenación es primero por ORDEN_PRIORIDAD (ascendente), luego por SMI_HOY (descendente)
    return (data.get('ORDEN_PRIORIDAD', 99), -data.get('SMI_HOY', 0))

def enviar_email(html_body, asunto, destinatario_email, nombre_usuario, fecha_asunto, hora_asunto):
    """Envía el correo electrónico con el reporte HTML."""
    try:
        sender_email = os.getenv('SENDER_EMAIL')
        sender_password = os.getenv('SENDER_PASSWORD')

        if not sender_email or not sender_password:
            print("❌ Error: Variables de entorno SENDER_EMAIL o SENDER_PASSWORD no configuradas.")
            return

        msg = MIMEMultipart("alternative")
        msg['Subject'] = asunto
        msg['From'] = sender_email
        msg['To'] = destinatario_email

        # Crea la parte de texto plano y la parte HTML
        text = f"Hola {nombre_usuario},\n\nAquí está tu reporte de análisis premium de hoy.\n\nPor favor, visualiza el correo en un cliente que soporte HTML para ver la tabla interactiva.\n\nAsunto: {asunto}"
        
        # Adjunta el contenido HTML
        part_html = MIMEText(html_body, 'html', 'utf-8')
        msg.attach(part_html)

        # Conexión y envío del correo
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, destinatario_email, msg.as_string())
            
        print(f"✅ Correo enviado con éxito a {destinatario_email} para {nombre_usuario}.")

    except Exception as e:
        print(f"❌ Error al enviar el correo a {destinatario_email}: {e}")

# ----------------------------------------------------------------------
# FUNCIÓN PRINCIPAL
# ----------------------------------------------------------------------
def generar_reporte():
    """Función principal para leer usuarios, obtener datos, clasificar y enviar reportes."""
    print("Iniciando el proceso de generación y envío de reportes premium...")
    
    try:
        # 1. Leer la lista de usuarios, emails, planes y empresas desde Google Sheets
        usuarios_data = leer_google_sheets()

        # Configurar la fecha y hora para el asunto del correo
        ahora = datetime.now()
        fecha_asunto = ahora.strftime('%d/%m')
        # La hora se redondea al inicio de la hora actual
        hora_asunto = ahora.strftime('%H:00') 

        for usuario in usuarios_data:
            if len(usuario) < 4:
                print(f"⚠️ Advertencia: Fila incompleta encontrada: {usuario}. Saltando...")
                continue

            nombre_usuario, email_usuario, plan_usuario, empresas_usuario_str = usuario
            
            # ------------------------------------------------------------------
            # CORRECCIÓN: Se elimina el filtro de plan para procesar a todos
            # los usuarios listados en el Google Sheet.
            # ------------------------------------------------------------------
            # if plan_usuario.upper() != 'PREMIUM':
            #     print(f"ℹ️ Usuario {nombre_usuario} (Plan: {plan_usuario}) no es Premium. Saltando...")
            #     continue
            # ------------------------------------------------------------------

            # Limpieza y mapeo de empresas
            empresas_nombres = [e.strip() for e in empresas_usuario_str.split(',') if e.strip()]
            
            if not empresas_nombres:
                print(f"⚠️ Usuario {nombre_usuario} no ha especificado empresas. Saltando envío...")
                continue
                
            print(f"\nProcesando usuario: {nombre_usuario} (Email: {email_usuario}, Plan: {plan_usuario}, Empresas: {len(empresas_nombres)})")

            try:
                datos_para_reporte = []
                
                for nombre_empresa in empresas_nombres:
                    ticker = tickers.get(nombre_empresa)
                    
                    if not ticker:
                        print(f"⚠️ Ticker no encontrado para la empresa: {nombre_empresa}. Generando análisis fallido.")
                        # --------------------------------------------------------------------------
                        # Manejo de empresas no encontradas
                        datos_para_reporte.append({
                            "TICKER": "N/A", "NOMBRE_EMPRESA": nombre_empresa, "PRECIO_ACTUAL": "N/A", 
                            "SMI_AYER": "N/A", "SMI_HOY": 0.0, "TENDENCIA_ACTUAL": "N/A", 
                            "ESTADO_SMI": "ANÁLISIS FALLIDO", "PRECIO_APLANAMIENTO": "N/A", 
                            "PENDIENTE": 0, "COMPRADO": "N/A", "PRECIO_COMPRA": "N/A", 
                            "FECHA_COMPRA": "N/A", "HIST_DF": None, "SOPORTE_1": "N/A", 
                            "SOPORTE_2": "N/A", "RESISTENCIA_1": "N/A", "TIPO_EMA": "N/A", 
                            "VALOR_EMA": "N/A", "RESISTENCIA_2": "N/A", "SMI_SEMANAL": "N/A", 
                            "ESTADO_SMI_SEMANAL": "N/A", "ADVERTENCIA_SEMANAL": "NO", 
                            "OBSERVACION_SEMANAL": "Empresa no listada en el diccionario de tickers.",
                            "OPORTUNIDAD": "ANÁLISIS FALLIDO", "COMPRA_SI": "N/A", "VENDE_SI": "N/A",
                            "ORDEN_PRIORIDAD": 99, 
                            "OBSERVACION_FINAL": "Empresa no encontrada en la lista de Tickers. Revisa el nombre."
                        })
                        # --------------------------------------------------------------------------
                        continue

                    # 2. Obtener datos de yFinance
                    data = obtener_datos_yfinance(ticker)
                    
                    if not data:
                        print(f"⚠️ No se pudieron obtener datos para {nombre_empresa} ({ticker}). Generando análisis fallido.")
                        # --------------------------------------------------------------------------
                        # Manejo de fallos en yFinance
                        datos_para_reporte.append({
                            "TICKER": ticker, "NOMBRE_EMPRESA": nombre_empresa, "PRECIO_ACTUAL": "N/A", 
                            "SMI_AYER": "N/A", "SMI_HOY": 0.0, "TENDENCIA_ACTUAL": "N/A", 
                            "ESTADO_SMI": "ANÁLISIS FALLIDO", "PRECIO_APLANAMIENTO": "N/A", 
                            "PENDIENTE": 0, "COMPRADO": "N/A", "PRECIO_COMPRA": "N/A", 
                            "FECHA_COMPRA": "N/A", "HIST_DF": None, "SOPORTE_1": "N/A", 
                            "SOPORTE_2": "N/A", "RESISTENCIA_1": "N/A", "TIPO_EMA": "N/A", 
                            "VALOR_EMA": "N/A", "RESISTENCIA_2": "N/A", "SMI_SEMANAL": "N/A", 
                            "ESTADO_SMI_SEMANAL": "N/A", "ADVERTENCIA_SEMANAL": "NO", 
                            "OBSERVACION_SEMANAL": "Error al obtener datos financieros.",
                            "OPORTUNIDAD": "ANÁLISIS FALLIDO", "COMPRA_SI": "N/A", "VENDE_SI": "N/A",
                            "ORDEN_PRIORIDAD": 99,
                            "OBSERVACION_FINAL": "No se pudieron obtener datos históricos o el ticker es inválido."
                        })
                        # --------------------------------------------------------------------------
                        continue
                        
                    # 3. Clasificar oportunidad y generar observaciones
                    data = clasificar_empresa(data)
                    data = generar_observaciones(data)
                    
                    datos_para_reporte.append(data)
                
                
                if not datos_para_reporte:
                    print(f"⚠️ Usuario {nombre_usuario} no tiene empresas válidas o no se encontraron datos. Saltando envío...")
                    continue
                    

                    
                # 4. ORDENAR DATOS Y GENERAR HTML PERSONALIZADO
                # La ordenación ahora incluye la nueva prioridad para fallos (99)
                datos_ordenados = sorted(datos_para_reporte, key=obtener_clave_ordenacion)

                # Generar el HTML personalizado
                html_body = generar_html_reporte(datos_ordenados, nombre_usuario)

                # 5. ENVIAR CORREO PERSONALIZADO
                # ASUNTO CON EL FORMATO REQUERIDO: "ANALISIS PREMIUM 30/09 17:00 horas."
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
