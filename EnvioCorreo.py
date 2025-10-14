import os
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ======================================================================
# 1. LECTURA DE GOOGLE SHEETS (Función simplificada)
# Solo necesitamos Nombre (A) y Email (B).
# Se usan las mismas variables de entorno que en el código original.
# ======================================================================
def leer_google_sheets_simplificado():
    """Lee la lista de usuarios (Nombre y Email) desde Google Sheets."""
    try:
        # Cargar credenciales desde la variable de entorno
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
    
    # Rango: A: Nombre, B: Email. Se asume Hoja 1, empezando en Fila 2 (A2)
    range_name = 'Hoja 1!A2:B' 

    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
    values = result.get('values', [])

    if not values:
        print('No se encontraron usuarios en la lista.')
    else:
        print(f'Se encontraron {len(values)} usuarios para procesar.')
        
    # Devuelve: [['Nombre', 'Email'], ...]
    return values 

# ======================================================================
# 2. FUNCIÓN DE ENVÍO DE CORREO SIMPLIFICADA (Datos tomados de Premium.py)
# ======================================================================
def enviar_email_simple(destinatario_usuario, nombre_usuario):
    """Envía un correo con el cuerpo 'HOLA', utilizando las credenciales SMTP de tu código original."""
    
    # --- 1. CREDENCIALES DE ENVÍO SMTP (Brevo) ---
    # DATOS LITERALES COPIADOS DE LA FUNCIÓN 'enviar_email' EN Premium.py
    servidor_smtp = 'smtp-relay.brevo.com'
    puerto_smtp = 587 
    remitente_nombre_completo = "IBEXIA.es <info@ibexia.es>" 
    remitente_visible = "info@ibexia.es" 
    remitente_login = "9853a2001@smtp-brevo.com" 
    password = "PRHTU5GN1ygZ9XVC"  
    
    asunto_email = "Mensaje de Prueba IBEXIA - HOLA"
    
    # --- 2. GENERACIÓN DEL CUERPO MÍNIMO DEL CORREO ---
    # Cuerpo del email requerido: "HOLA"
    cuerpo_aviso_html = f"""
    <div style="max-width: 600px; margin: 0 auto; padding: 20px; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; color: #343a40; text-align: left;"> 
        <h2 style="color: #495057; font-size: 1.5em; margin-bottom: 20px;">
            👋 ¡Hola, {nombre_usuario}!
        </h2>
        
        <p style="font-size: 1.2em; font-weight: bold; color: #007bff; margin-bottom: 30px;">
            HOLA
        </p>
        <p style="font-size: 0.9em; color: #6c757d; margin-top: 20px;">
            Este es un mensaje de prueba simplificado desde tu script.
        </p>
    </div>
    """

    # --- 3. CONFIGURACIÓN DEL MENSAJE ---
    msg = MIMEMultipart('alternative') 
    
    msg['From'] = remitente_nombre_completo 
    msg['To'] = destinatario_usuario 
    msg['Subject'] = asunto_email

    # Adjuntar el HTML como cuerpo del mensaje
    part = MIMEText(cuerpo_aviso_html, 'html')
    msg.attach(part)
    
    # --- 4. ENVÍO SMTP ---
    try:
        servidor = smtplib.SMTP(servidor_smtp, puerto_smtp)
        servidor.starttls() 
        servidor.login(remitente_login, password) 
        servidor.sendmail(remitente_visible, destinatario_usuario, msg.as_string()) 
        servidor.quit()
        print(f"✅ Correo 'HOLA' enviado a {destinatario_usuario} (Nombre: {nombre_usuario})")
        
    except Exception as e:
        print(f"❌ Error al enviar el correo a {destinatario_usuario}: {e}")

# ======================================================================
# 3. LÓGICA PRINCIPAL SIMPLIFICADA
# ======================================================================
def enviar_saludos_a_usuarios():
    try:
        print("Iniciando lectura de usuarios y envío de correos 'HOLA'...")
        usuarios_premium = leer_google_sheets_simplificado()

        for usuario in usuarios_premium:
            try:
                # Se espera el formato [Nombre, Email]
                if len(usuario) < 2:
                    print(f"⚠️ Fila con formato incorrecto o incompleto: {usuario}. Saltando...")
                    continue
                    
                nombre_usuario = usuario[0].strip()
                email_usuario = usuario[1].strip()
                
                # Simple validación básica
                if not nombre_usuario or not email_usuario:
                    print(f"⚠️ Fila incompleta (Nombre o Email vacío). Saltando...")
                    continue

                enviar_email_simple(email_usuario, nombre_usuario)

            except Exception as e:
                print(f"❌ Error al procesar y enviar correo al usuario {usuario}: {e}")

        print("\nProceso de envío de saludos completado.")

    except Exception as e:
        print(f"❌ Error al ejecutar el script principal de saludos: {e}")

if __name__ == '__main__':
    enviar_saludos_a_usuarios()