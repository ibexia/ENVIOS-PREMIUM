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
        print(f'Se encontraron {len(values)} usuarios premium para procesar.')
        
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
        else:
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
        else:
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
        else:
            data['OPORTUNIDAD'] = "Intermedio"
            data['COMPRA_SI'] = "NO PREVEEMOS GIRO EN ESTOS MOMENTOS"
            data['VENDE_SI'] = "NO PREVEEMOS GIRO EN ESTOS MOMENTOS"
            data['ORDEN_PRIORIDAD'] = prioridad["Intermedio"]
    
    return data
    
def generar_observaciones(data):
    nombre_empresa = data['NOMBRE_EMPRESA']
    precio_actual = formatear_numero(data['PRECIO_ACTUAL'])
    estado_smi = data['ESTADO_SMI']
    tendencia = data['TENDENCIA_ACTUAL']
    oportunidad = data['OPORTUNIDAD']
    soporte1 = formatear_numero(data['SOPORTE_1'])
    resistencia1 = formatear_numero(data['RESISTENCIA_1'])
    compra_si = data['COMPRA_SI']
    vende_si = data['VENDE_SI']
    tipo_ema = data['TIPO_EMA']
    valor_ema = formatear_numero(data['VALOR_EMA'])
    
    # --- Nuevo: Advertencia Semanal ---
    advertencia_semanal = data['ADVERTENCIA_SEMANAL']

    # CAMBIO: Se añade style='text-align:left' a la etiqueta p exterior
    texto_observacion = f'<strong style="text-align:left;">Observaciones de {nombre_empresa}:</strong><br>'
    
    # Nuevo texto de advertencia para insertar al inicio
    advertencia_texto = ""
    if advertencia_semanal == "SI":
        advertencia_texto = "<strong style='color:#ffc107;'>ADVERTENCIA SEMANAL: El SMI semanal está en zona de sobrecompra. No se recomienda comprar ya que la subida podría ser muy corta y con alto riesgo.</strong><br>"


    if oportunidad == "Posibilidad de Compra Activada":
        texto = f"El algoritmo se encuentra en una zona de sobreventa y muestra una tendencia alcista en sus últimos valores, lo que activa una señal de compra fuerte. Se recomienda tener en cuenta los niveles de resistencia ({resistencia1}€) para determinar un objetivo de precio. La EMA de 100 periodos se encuentra en {valor_ema}€, actuando como un nivel de {tipo_ema}."
    
    elif oportunidad == "Posibilidad de Compra":
        if "COMPRA YA" in compra_si:
            texto = f"El algoritmo detecta que el valor está en una zona de sobreventa, lo que puede ser un indicador de reversión. El algoritmo ha detectado una oportunidad de compra inmediata para aprovechar un posible rebote.La EMA de 100 periodos se encuentra en {valor_ema}€, actuando como un nivel de {tipo_ema}."
        else:
            texto = f"El algoritmo detecta que el valor está en una zona de sobreventa con una tendencia bajista. Se ha detectado una oportunidad de {compra_si} para un posible rebote. La EMA de 100 periodos se encuentra en {valor_ema}€, actuando como un nivel de {tipo_ema}."
    
    # Nuevo bloque de Riesgo de Compra
    elif oportunidad == "Compra RIESGO":
        texto = f"El algoritmo detectó una señal de compra diaria, pero el **SMI Semanal** se encuentra en zona de **Sobrecompra** ({formatear_numero(data['SMI_SEMANAL'])}). Esto indica que el precio ya ha subido mucho a largo plazo, y la señal de rebote diaria podría ser muy breve. No se recomienda la compra en este momento. La EMA de 100 periodos se encuentra en {valor_ema}€, actuando como un nivel de {tipo_ema}."

    elif oportunidad == "VIGILAR":
        texto = f"El algoritmo se encuentra en una zona intermedia y muestra una tendencia alcista en sus últimos valores. Se sugiere vigilar de cerca, ya que una caída en el precio podría ser una señal de venta. {vende_si}. Se recomienda tener en cuenta los niveles de soporte ({soporte1}€) para saber hasta dónde podría bajar el precio. La EMA de 100 periodos se encuentra en {valor_ema}€, actuando como un nivel de {tipo_ema}."
    
    elif oportunidad == "Riesgo de Venta":
        texto = f"El algoritmo ha entrado en una zona de sobrecompra. Esto genera un riesgo de venta. Se recomienda tener en cuenta los niveles de soporte ({soporte1}€) para saber hasta dónde podría bajar el precio. La EMA de 100 periodos se encuentra en {valor_ema}€, actuando como un nivel de {tipo_ema}."
    
    elif oportunidad == "Riesgo de Venta Activada":
        texto = f"La combinación de una zona de sobrecompra y una tendencia bajista en el algoritmo ha activado una señal de riesgo de venta. Se recomienda tener en cuenta los niveles de soporte ({soporte1}€) para saber hasta dónde podría bajar el precio. La EMA de 100 periodos se encuentra en {valor_ema}€, actuando como un nivel de {tipo_ema}."

    elif oportunidad == "Seguirá bajando":
        texto = f"El algoritmo sugiere que es probable que el precio siga bajando en el corto plazo. No se aconseja ni comprar ni vender. Se recomienda observar los niveles de soporte ({soporte1}€). La EMA de 100 periodos se encuentra en {valor_ema}€, actuando como un nivel de {tipo_ema}."

    elif oportunidad == "Intermedio":
        texto = "El algoritmo no emite recomendaciones de compra o venta en este momento, por lo que lo más prudente es mantenerse al margen. La EMA de 100 periodos se encuentra en {valor_ema}€, actuando como un nivel de {tipo_ema}."
    
    else:
        texto = "El algoritmo se encuentra en una zona de sobreventa y muestra una tendencia alcista en sus últimos valores, lo que activa una señal de compra fuerte. Se recomienda comprar para aprovechar un posible rebote, con un objetivo de precio en la zona de resistencia. La EMA de 100 periodos se encuentra en {valor_ema}€, actuando como un nivel de {tipo_ema}."
    
    # Se añade la advertencia al inicio del texto de la observación
    return f'<p style="text-align:left; color:#000;">{texto_observacion.strip()}{advertencia_texto}{texto.strip()}</p>'

def obtener_clave_ordenacion(empresa):
    categoria = empresa['OPORTUNIDAD']
    nombre = empresa.get('NOMBRE_EMPRESA', '')

    # ⚠️ DIAGNÓSTICO: FORZAR ACERINOX y ACS al principio (Prioridad 0)
    if nombre == 'Acerinox' or nombre == 'ACS':
        return (0, nombre)
    
    prioridad = {
        "Posibilidad de Compra Activada": 1,
        "Posibilidad de Compra": 2,
        "Compra RIESGO": 2.5,
        "VIGILAR": 3,
        "Riesgo de Venta": 4,
        "Riesgo de Venta Activada": 5,
        "Seguirá bajando": 6,
        "Intermedio": 7,
        "ANÁLISIS FALLIDO": 99 
    }

    orden_interna = prioridad.get(categoria, 99)
    return (orden_interna, empresa['NOMBRE_EMPRESA'])

# ----------------------------------------------------------------------
# 2.1 NUEVA FUNCIÓN: GENERACIÓN DE UNA FILA DE REPORTE DE ERROR
# ----------------------------------------------------------------------
def generar_fila_reporte_error(data):
    """Genera una fila de reporte simple para empresas que fallaron en la obtención/análisis de datos."""
    
    # Se utiliza una columna para el nombre y el mensaje de error en las otras.
    return f"""
                <tr class="main-row error-row" style="background-color: #f8d7da; color: #721c24;">
                    <td class="red-cell" style="font-weight: bold;">{data['NOMBRE_EMPRESA']}</td>
                    <td colspan="4" style="text-align: left; padding: 10px;">
                        ❌ {data['MOTIVO_FALLO']}
                    </td>
                </tr>
    """

# ----------------------------------------------------------------------
# 2.2 FUNCIÓN MODIFICADA: GENERACIÓN DE UNA FILA DE REPORTE HTML
# ----------------------------------------------------------------------


def generar_fila_reporte(data):
    """Genera la fila principal, la fila de detalle y la fila de observaciones para una empresa, usando estilos inline para máxima compatibilidad con email."""
    
    global tickers
    nombre_empresa_url = None
    for nombre, ticker_val in tickers.items():
        if ticker_val == data['TICKER']:
            nombre_empresa_url = nombre
            break
            
    if nombre_empresa_url:
        # Enlace a tu web (se mantiene)
        empresa_link = f'https://ibexia.es/category/{nombre_empresa_url.lower()}/'
    else:
        empresa_link = '#'
        
    # Colores y estilos INLINE (CLAVE para la visibilidad en emails)
    colores = {
        "COMPRA": "#d4edda",        # Verde claro fuerte (Celda Nombre)
        "RIESGO": "#fff3cd",        # Amarillo (Celda Nombre)
        "VENTA": "#f8d7da",         # Rojo claro (Celda Nombre)
        "DEFAULT_CELDA": "#f9f9f9"  # Gris muy claro (para Intermedio/Vigilar)
    }
    
    # 1. Determinar color de fondo para la celda del nombre (la que lleva color de clase)
    if "compra" in data['OPORTUNIDAD'].lower() and "riesgo" not in data['OPORTUNIDAD'].lower():
        color_celda_nombre = colores["COMPRA"]
    elif "venta" in data['OPORTUNIDAD'].lower():
        color_celda_nombre = colores["VENTA"]
    elif "riesgo" in data['OPORTUNIDAD'].lower(): # Compra RIESGO
        color_celda_nombre = colores["RIESGO"]
    else:
        # Aquí caen ACX y ACS ("Intermedio", "VIGILAR", etc.)
        # Asignamos un color CLARO, pero explícito, para garantizar la visibilidad
        color_celda_nombre = colores["DEFAULT_CELDA"] 
    
    # 2. Determinar color de fondo para la fila principal (ligero, visible para el cliente de correo)
    color_fondo_fila = "#ffffff" # Por defecto blanco, pero la celda Nombre lleva el color

    # 3. Determinar el color de la celda OPORTUNIDAD (la celda central)
    # Utilizamos el mismo mapeo para darle el color directamente al contenido
    if "compra" in data['OPORTUNIDAD'].lower() and "riesgo" not in data['OPORTUNIDAD'].lower():
        color_texto_oportunidad = "#155724" # Texto oscuro
    elif "venta" in data['OPORTUNIDAD'].lower():
        color_texto_oportunidad = "#721c24" # Texto rojo oscuro
    elif "riesgo" in data['OPORTUNIDAD'].lower():
        color_texto_oportunidad = "#856404" # Texto amarillo oscuro
    else:
        color_texto_oportunidad = "#383d41" # Texto gris oscuro/negro
    
    
    # Se mantienen los estilos de la celda de la empresa
    nombre_con_precio = f"<a href='{empresa_link}' target='_blank' style='text-decoration:none; color:inherit;'><div style='padding: 5px 0;'><b>{data['NOMBRE_EMPRESA']}</b><br>({formatear_numero(data['PRECIO_ACTUAL'])}€)</div></a>"

    observaciones = generar_observaciones(data)
    
    # --- FILAS DE REPORTE CON ESTILOS INLINE (SOLUCIÓN A LA OCULTACIÓN) ---
    return f"""
                <tr class="main-row" style="background-color: {color_fondo_fila}; border-bottom: 1px solid #ddd;">
                    <td style="background-color: {color_celda_nombre}; font-weight: bold; padding: 5px 10px; border-right: 1px solid #ddd; text-align: left; vertical-align: middle;">{nombre_con_precio}</td>
                    
                    <td style="padding: 10px; border-right: 1px solid #ddd; text-align: center; vertical-align: middle;">{data['TENDENCIA_ACTUAL']}</td>
                    
                    <td style="font-weight: bold; padding: 10px; border-right: 1px solid #ddd; text-align: center; color: {color_texto_oportunidad}; vertical-align: middle;">{data['OPORTUNIDAD']}</td>
                    
                    <td style="padding: 10px; border-right: 1px solid #ddd; text-align: center; vertical-align: middle;">{data['COMPRA_SI']}</td>
                    
                    <td style="padding: 10px; text-align: center; vertical-align: middle;">{data['VENDE_SI']}</td>
                </tr>
                
                <tr class="detailed-row">
                    <td colspan="5" style="padding: 10px; border-top: 1px solid #eee; background-color: #ffffff;">
                        <div style="display:flex; justify-content:space-around; align-items:flex-start; font-size: 0.9em; line-height: 1.5;">
                            <div style="flex-basis: 25%; text-align:left;">
                                <b>EMA</b><br>
                                <span style="font-weight:bold;">{formatear_numero(data['VALOR_EMA'])}€</span><br>
                                ({data['TIPO_EMA']})
                            </div>
                            <div style="flex-basis: 25%; text-align:left;">
                                <b>Soportes</b><br>
                                S1: {formatear_numero(data['SOPORTE_1'])}€<br>
                                S2: {formatear_numero(data['SOPORTE_2'])}€
                            </div>
                            <div style="flex-basis: 25%; text-align:left;">
                                <b>Resistencias</b><br>
                                R1: {formatear_numero(data['RESISTENCIA_1'])}€<br>
                                R2: {formatear_numero(data['RESISTENCIA_2'])}€
                            </div>
                            <div style="flex-basis: 25%; text-align:left; font-size:0.9em;">
                                <b>Análisis Semanal (SMI)</b><br>
                                {data['OBSERVACION_SEMANAL']}
                            </div>
                        </div>
                    </td>
                </tr>
                <tr class="observaciones-row">
                    <td colspan="5" style="padding: 10px; border-bottom: 3px solid #ddd; background-color: #f4f4f4; text-align: left;">{observaciones}</td>
                </tr>
    """

# ----------------------------------------------------------------------
# 3. FUNCIÓN MODIFICADA: GENERACIÓN DEL CUERPO HTML COMPLETO
# Se añade el bloque de búsqueda (searchInput) y el script filterTable.
# ----------------------------------------------------------------------
def generar_html_reporte(datos_ordenados, nombre_usuario):
    """Genera el cuerpo HTML completo con datos y estilos."""

    tabla_html_contenido = ""
    previous_orden_grupo = None
    
# --- MODIFICACIÓN EN GENERAR_HTML_REPORTE (BUCLE FOR) ---
    for i, data in enumerate(datos_ordenados):
        # NOTA: Usar 'OPORTUNIDAD' de data para obtener la clave, incluso para 'ANÁLISIS FALLIDO'
        # La función obtener_clave_ordenacion devolverá 99 para 'ANÁLISIS FALLIDO'
        current_orden_grupo = obtener_clave_ordenacion(data)[0] 
        
        # Lógica para determinar el encabezado de categoría
        es_primera_fila = previous_orden_grupo is None
        es_cambio_grupo = current_orden_grupo != previous_orden_grupo
        
        if es_primera_fila or es_cambio_grupo:
            
            # MODIFICACIÓN DE LA LÓGICA DE ENCABEZADO
            if current_orden_grupo in [1, 2, 2.5]: # Grupo de Compra
                if previous_orden_grupo is None or previous_orden_grupo not in [1, 2, 2.5]:
                    tabla_html_contenido += """
                        <tr class="category-header"><td colspan="5">OPORTUNIDADES DE COMPRA</td></tr>
                    """
            elif current_orden_grupo in [3, 4, 5]: # Grupo de Venta/Vigilancia
                if previous_orden_grupo is None or previous_orden_grupo not in [3, 4, 5]:
                    tabla_html_contenido += """
                        <tr class="category-header"><td colspan="5">ATENTOS A VENDER/VIGILANCIA</td></tr>
                    """
            elif current_orden_grupo in [6, 7]: # Grupo Intermedio
                if previous_orden_grupo is None or previous_orden_grupo not in [6, 7]:
                    tabla_html_contenido += """
                        <tr class="category-header"><td colspan="5">OTRAS EMPRESAS SIN MOVIMIENTOS</td></tr>
                    """
            # NUEVO BLOQUE: ENCABEZADO PARA ERRORES (Prioridad 99)
            elif current_orden_grupo == 99: 
                if previous_orden_grupo is None or previous_orden_grupo != 99:
                    tabla_html_contenido += """
                        <tr class="category-header" style="background-color: #dc3545;"><td colspan="5">❌ EMPRESAS CON PROBLEMAS EN EL ANÁLISIS</td></tr>
                    """
                    
            # Poner un separador si no es la primera fila y hay cambio de grupo
            if not es_primera_fila and es_cambio_grupo:
                tabla_html_contenido += """
                    <tr class="separator-row"><td colspan="5"></td></tr>
                """

        # AGREGAR LAS FILAS DE LA EMPRESA (Usar la función correcta: error o reporte normal)
        if data['OPORTUNIDAD'] == "ANÁLISIS FALLIDO":
             tabla_html_contenido += generar_fila_reporte_error(data)
        else:
             tabla_html_contenido += generar_fila_reporte(data)
             
        previous_orden_grupo = current_orden_grupo

    # --- INICIO DEL GRAN STRING HTML ---
    now_utc = datetime.utcnow()
    hora_actual = (now_utc + timedelta(hours=2)).strftime('%H:%M')
    
    html_body = f"""
        <html>
        <head>
            <title>Reporte Premium IBEXIA - {datetime.today().strftime('%d/%m/%Y')}</title>
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
                    /* CAMBIO AÑADIDO: Asegura alineación general de texto a la izquierda en el cuerpo del correo. */
                    text-align: left; 
                }}
                h2 {{
                    color: #343a40;
                    text-align: center;
                    font-size: 1.5em;
                    margin-bottom: 10px;
                }}
                p {{
                    color: #6c757d;
                    /* CAMBIO: Se remueve el text-align: center por defecto en párrafos. */
                    text-align: left; 
                    font-size: 0.9em;
                }}
                #search-container {{
                    margin-bottom: 15px;
                }}
                #searchInput {{
                    width: 100%;
                    padding: 8px;
                    font-size: 0.9em;
                    border: 1px solid #ced4da;
                    border-radius: 4px;
                    box-sizing: border-box;
                }}
                .table-container {{
                    /* Se eliminan propiedades relacionadas con scroll y posición fija, pues en email no funcionan bien */
                    overflow-x: auto;
                    overflow-y: hidden; 
                    position: relative;
                }}
                table {{
                    width: 100%;
                    table-layout: fixed;
                    margin: 10px auto 0 auto; /* Mantener la tabla centrada */
                    border-collapse: collapse;
                    font-size: 0.85em;
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
                    /* Se eliminan propiedades sticky */
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
                
                /* Se elimina la clase collapsible-row y expand-button */
                .detailed-row td {{
                    padding: 0;
                }}
                
                /* Estilos para ocultar/mostrar filas */
                .hidden-row {{
                    display: none;
                }}
            </style>
        </head>
        <body>
            <div class="main-container">
                <h2 class="text-center">👋 ¡Hola, {nombre_usuario}! Tu Reporte Premium de Oportunidades - {datetime.today().strftime('%d/%m/%Y')} {hora_actual}</h2>
                <p>Aquí tienes el análisis actualizado de las {len(datos_ordenados)} empresas que has elegido:</p>
                
                <div id="search-container">
                    <input type="text" id="searchInput" onkeyup="filterTable()" placeholder="Buscar por empresa o categoría...">
                </div>
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
                            {tabla_html_contenido}
                        </tbody>
                    </table>
                </div>
                
                <br>
                <p class="disclaimer"><strong>Aviso:</strong> El algoritmo de trading se basa en indicadores técnicos y no garantiza la rentabilidad. Utiliza esta información con tu propio análisis y criterio. ¡Feliz trading!</p>
            </div>

            <script>
                function filterTable() {{
                    var input, filter, table, tbody, rows, i, j, txtValue, mainRow, detailRow, obsRow;
                    input = document.getElementById("searchInput");
                    filter = input.value.toUpperCase();
                    table = document.getElementById("myTable");
                    tbody = table.getElementsByTagName("tbody")[0];
                    rows = tbody.rows; // Incluye main-row, detailed-row, observaciones-row, category-header, separator-row

                    for (i = 0; i < rows.length; i++) {{
                        mainRow = rows[i];
                        
                        // Ignorar filas de encabezado y separador, mantenerlas siempre visibles si no es error
                        if (mainRow.classList.contains('category-header') || mainRow.classList.contains('separator-row')) {{
                            mainRow.style.display = "";
                            continue;
                        }}
                        
                        // Solo procesar las filas que contienen los datos principales
                        if (mainRow.classList.contains('main-row')) {{
                            
                            // Obtener las filas asociadas: detailed-row es i+1, observaciones-row es i+2
                            detailRow = (i + 1 < rows.length && rows[i+1].classList.contains('detailed-row')) ? rows[i+1] : null;
                            obsRow = (i + 2 < rows.length && rows[i+2].classList.contains('observaciones-row')) ? rows[i+2] : null;
                            
                            // Obtener el texto de la primera celda (Nombre Empresa) y la tercera (Oportunidad)
                            // Se asume que la celda 0 contiene el nombre de la empresa y 2 la oportunidad
                            var cells = mainRow.getElementsByTagName("td");
                            
                            var match = false;
                            
                            // Búsqueda en Nombre Empresa (Celda 0) y Oportunidad (Celda 2)
                            if (cells.length > 0) {{
                                // Nombre de la Empresa (Celda 0)
                                txtValue = cells[0].textContent || cells[0].innerText;
                                if (txtValue.toUpperCase().indexOf(filter) > -1) {{
                                    match = true;
                                }}
                                // Oportunidad (Celda 2)
                                txtValue = cells[2].textContent || cells[2].innerText;
                                if (txtValue.toUpperCase().indexOf(filter) > -1) {{
                                    match = true;
                                }}
                            }}

                            if (match) {{
                                mainRow.classList.remove("hidden-row");
                                if (detailRow) detailRow.classList.remove("hidden-row");
                                if (obsRow) obsRow.classList.remove("hidden-row");
                            }} else {{
                                mainRow.classList.add("hidden-row");
                                if (detailRow) detailRow.classList.add("hidden-row");
                                if (obsRow) obsRow.classList.add("hidden-row");
                            }}
                            
                            // Saltar a la siguiente fila principal (saltamos detailed-row y observaciones-row)
                            i += 2;
                        }}
                    }}
                    
                    // Lógica para mostrar/ocultar encabezados de categoría según si contienen filas visibles
                    // Esta lógica es más compleja y se deja fuera por defecto para evitar problemas en el email.
                    // En el navegador, los encabezados se mantendrán por simplicidad.
                }}
            </script>
            </body>
        </html>
    """
    # --- FIN DEL GRAN STRING HTML ---
    return html_body

# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# 4. FUNCIÓN MODIFICADA: ENVÍO DE CORREO
# Se mantiene la lógica de enviar el reporte como adjunto.
# ----------------------------------------------------------------------
def enviar_email(html_content_full_report, asunto_email, destinatario_usuario, nombre_usuario, fecha_asunto, hora_asunto):
    """Envía un correo minimalista con un aviso para abrir el HTML adjunto."""
    
    # --- 1. CREDENCIALES DE ENVÍO SMTP (Brevo) ---
    servidor_smtp = 'smtp-relay.brevo.com'
    puerto_smtp = 587 
    
    remitente_nombre_completo = "IBEXIA.es <info@ibexia.es>" 
    remitente_visible = "info@ibexia.es" 
    
    # Credenciales de Brevo (Asumo que son variables de entorno o constantes privadas)
    # ATENCIÓN: No incluyo las claves aquí. Debes mantener tus constantes.
    remitente_login = "9853a2001@smtp-brevo.com" 
    password = "PRHTU5GN1ygZ9XVC"  
    
    # --- 2. GENERACIÓN DEL CUERPO MÍNIMO DEL CORREO ---
    # Este es el texto profesional que se verá dentro de Gmail
    cuerpo_aviso_html = f"""
    <div style="max-width: 600px; margin: 0 auto; padding: 20px; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; color: #343a40; text-align: left;"> 
        <h2 style="color: #495057; font-size: 1.5em; margin-bottom: 20px;">
            👋 ¡Hola, {nombre_usuario}! Tu Reporte Premium de Oportunidades
        </h2>
        <p style="font-size: 1.0em; margin-bottom: 25px;">
            Le confirmamos el envío de su <strong>Reporte Premium de Oportunidades Bursátiles</strong> de IBEXIA, correspondiente al <strong>{fecha_asunto} a las {hora_asunto} horas</strong>.
        </p>
        <p style="font-size: 1.0em; font-weight: bold; color: #007bff; margin-bottom: 30px;">
            Hemos adjuntado el análisis completo en formato HTML. Por favor, <strong style="color: #dc3545;">descargue y abra el archivo adjunto</strong> directamente en su navegador web para visualizar el reporte completo con la funcionalidad y estilos correctos.
        </p>
        <p style="font-size: 0.9em; color: #6c757d; margin-top: 20px;">
            Para cualquier consulta sobre su análisis, contacte con nuestro equipo de soporte: <a href="mailto:info@ibexia.es" style="color: #007bff; text-decoration: none;">info@ibexia.es</a>.
        </p>
        <div style="margin-top: 30px; border-top: 1px solid #eee; padding-top: 10px; font-size: 0.8em; text-align: center; color: #6c757d;">
            <strong>Aviso:</strong> El algoritmo de trading se basa en indicadores técnicos y no garantiza la rentabilidad.
        </div>
    </div>
    """

    # --- 3. CONFIGURACIÓN DEL MENSAJE Y ADJUNTOS ---
    msg = MIMEMultipart('mixed') 
    
    msg['From'] = remitente_nombre_completo 
    msg['To'] = destinatario_usuario 
    msg['Subject'] = asunto_email

    # Contenedor para el cuerpo del mensaje (el aviso minimalista)
    msg_body = MIMEMultipart('alternative')
    
    # Adjuntar el HTML de aviso como cuerpo del mensaje
    part = MIMEText(cuerpo_aviso_html, 'html')
    msg_body.attach(part)
    msg.attach(msg_body)
    
    # --- 4. LÓGICA PARA ADJUNTAR EL ARCHIVO HTML COMPLETO ---
    try:
        # Usamos el html_content_full_report para el archivo adjunto
        nombre_archivo_html = f"Reporte_Premium_{nombre_usuario}_{fecha_asunto.replace('/', '-')}_{hora_asunto.replace(':', '-')}.html"
        
        attachment = MIMEBase('application', 'octet-stream')
        # USAMOS EL CONTENIDO COMPLETO DEL REPORTE PARA EL ADJUNTO
        attachment.set_payload(html_content_full_report.encode('utf-8')) 
        encoders.encode_base64(attachment)
        attachment.add_header('Content-Disposition', 'attachment', filename=nombre_archivo_html)
        
        msg.attach(attachment)
        print(f"✔️ DEBUG: Adjuntando el archivo HTML COMPLETO {nombre_archivo_html} al correo.")

    except Exception as e:
        print(f"⚠️ Advertencia: No se pudo adjuntar el archivo HTML: {e}")
    # -----------------------------------------------------------


    # --- 5. ENVÍO SMTP (Se mantiene la lógica original) ---
    try:
        servidor = smtplib.SMTP(servidor_smtp, puerto_smtp)
        servidor.starttls() 
        servidor.login(remitente_login, password) 
        servidor.sendmail(remitente_visible, destinatario_usuario, msg.as_string()) 
        servidor.quit()
        print(f"✅ Correo enviado a {destinatario_usuario} desde {remitente_visible} con el asunto: {asunto_email} (Vía Brevo)")
        
    except Exception as e:
        print(f"❌ Error al enviar el correo a {destinatario_usuario} desde {remitente_visible}: {e}")

# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# 5. FUNCIÓN REESTRUCTURADA: LÓGICA PRINCIPAL (Multi-Usuario)
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
        # NUEVO DICCIONARIO PARA ALMACENAR ERRORES EXPLÍCITAMENTE
        errores_por_ticker = {} 
        
        for empresa_nombre, ticker in tickers.items():
            print(f"-> Procesando {ticker}...")
            data = None # Inicializamos 'data'

            # --- FASE 1: OBTENCIÓN DE DATOS (yfinance) ---
            try:
                # Intentamos obtener los datos
                data = obtener_datos_yfinance(ticker)
                
                # Capturamos explícitamente si yfinance devuelve None (fallo silencioso)
                if data is None:
                    raise ValueError("obtener_datos_yfinance devolvió None (sin datos o precio).")
                    
            except Exception as e:
                # Capturamos fallos de conexión o yfinance que no dan datos válidos
                print(f"❌ ERROR DE OBTENCIÓN/CONEXIÓN para {ticker}: {e}. Registrando fallo.")
                errores_por_ticker[ticker] = {
                    "NOMBRE_EMPRESA": empresa_nombre,
                    "TICKER": ticker,
                    "OPORTUNIDAD": "ANÁLISIS FALLIDO",
                    "MOTIVO_FALLO": f"Fallo al obtener datos de yfinance: {str(e)[:100]}..."
                }
                # Si falla la obtención, NO podemos clasificar. Pasamos a la siguiente empresa.
                continue 
            
            # --- FASE 2: CLASIFICACIÓN Y ANÁLISIS (clasificar_empresa) ---
            try:
                # Si llegamos aquí, 'data' contiene el DataFrame de yfinance
                resultado_analisis = clasificar_empresa(data)
                
                # 1. Aseguramos que el resultado no sea None
                if resultado_analisis is None:
                    raise ValueError("clasificar_empresa devolvió None.")
                
                # 2. **NUEVO Y CRUCIAL:** Aseguramos que el resultado contenga la clave principal de clasificación
                if 'OPORTUNIDAD' not in resultado_analisis:
                    # Si falta esta clave, significa que la lógica de clasificación falló internamente.
                    raise KeyError("Falta la clave 'OPORTUNIDAD' en el resultado de la clasificación. Fallo interno del algoritmo.")
                    
                # Si todo está bien, guardamos el resultado
                datos_completos_por_ticker[ticker] = resultado_analisis
                print(f"✔️ OK: {ticker} clasificado y guardado.")

            except Exception as e:
                # Capturamos fallos en el cálculo o la lógica de clasificación (incluyendo los dos raise anteriores)
                print(f"❌ ERROR DE CLASIFICACIÓN/CÁLCULO para {ticker}: {e}. Registrando fallo de análisis.")
                errores_por_ticker[ticker] = {
                    "NOMBRE_EMPRESA": empresa_nombre,
                    "TICKER": ticker,
                    "OPORTUNIDAD": "ANÁLISIS FALLIDO",
                    "MOTIVO_FALLO": f"Fallo en la CLASIFICACIÓN/CÁLCULO: {str(e)[:100]}..."
                }


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
                


                # 3. DETERMINAR LOS TICKERS ESPECÍFICOS Y FORZAR LA INCLUSIÓN DE FALLOS
                plan_limpio = plan_usuario.upper().strip()
                datos_para_reporte = []
                
                if plan_limpio == 'LOTE':
                    # Si es LOTE, la lista de tickers que DEBERÍA tener es TODA
                    tickers_a_incluir = list(tickers.values())
                    print(f"DEBUG LOTE: Número total de tickers esperados: {len(tickers_a_incluir)}")
                else:
                    # Convertir la cadena de empresas a una lista de tickers válidos
                    nombres_elegidos = [n.strip() for n in empresas_str.split(',')]
                    tickers_a_incluir = [
                        tickers[nombre_largo] 
                        for nombre_largo in nombres_elegidos 
                        if nombre_largo in tickers
                    ]
                
                # ITERAR SOBRE LA LISTA DE TICKERS REQUERIDOS y FORZAR INCLUSIÓN
                for t in tickers_a_incluir:
                    if t in datos_completos_por_ticker:
                        # Empresa analizada correctamente
                        datos_para_reporte.append(datos_completos_por_ticker[t])
                    elif t in errores_por_ticker:
                        # Empresa que falló
                        # *** MODIFICACIÓN APLICADA AQUÍ: Solo incluimos fallos si el plan es LOTE ***
                        if plan_limpio == 'LOTE':
                             datos_para_reporte.append(errores_por_ticker[t])
                        # *** FIN DE MODIFICACIÓN ***

                # --------------------------------------------------------------------------
                
                
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
