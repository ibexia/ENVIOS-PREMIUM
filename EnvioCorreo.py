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
    """Envía un correo con el cuerpo modificado para informar sobre el cese del servicio Premium."""
    
    # --- 1. CREDENCIALES DE ENVÍO SMTP (Brevo) ---
    # DATOS LITERALES COPIADOS DE LA FUNCIÓN 'enviar_email' EN Premium.py
    servidor_smtp = 'smtp-relay.brevo.com'
    puerto_smtp = 587 
    remitente_nombre_completo = "IBEXIA.es <info@ibexia.es>" 
    remitente_visible = "info@ibexia.es" 
    remitente_login = "9853a2001@smtp-brevo.com" 
    # El código debe ser el guardado [2025-06-24]
    # Se utiliza la contraseña guardada: "PRHTU5GN1ygZ9XVC"
    password = "PRHTU5GN1ygZ9XVC"  
    
    # ASUNTO MODIFICADO
    asunto_email = "Actualización Importante sobre el Servicio Premium de IBEXIA.es"
    
    # --- 2. GENERACIÓN DEL CUERPO MÍNIMO DEL CORREO (MODIFICADO PROFESIONALMENTE) ---
    enlace_ibexia = "https://www.ibexia.es"
    
    cuerpo_aviso_html = f"""
    <div style="max-width: 600px; margin: 0 auto; padding: 20px; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; color: #343a40; text-align: left; line-height: 1.6; border: 1px solid #e9ecef; border-radius: 8px;"> 
        <h2 style="color: #495057; font-size: 1.5em; margin-bottom: 20px; border-bottom: 2px solid #007bff; padding-bottom: 10px;">
            Estimado/a Usuario/a, {nombre_usuario}
        </h2>
        
        <p style="margin-bottom: 15px;">
            Le escribimos desde <strong style="color: #007bff;">IBEXIA.es</strong> para comunicarle una **actualización estratégica** en la disponibilidad de nuestros servicios.
        </p>

        <p style="margin-bottom: 15px;">
            Tras un periodo de análisis y optimización, hemos tomado la decisión de **finalizar el servicio Premium** como una oferta independiente. Nuestro objetivo principal es maximizar el valor ofrecido a toda nuestra comunidad.
        </p>

        <p style="margin-bottom: 15px; font-weight: bold;">
            En consecuencia, nos complace informarle que las funcionalidades clave asociadas anteriormente al servicio Premium han sido integradas y ahora están disponibles de forma **completamente gratuita** a través de la sección principal de nuestro portal web.
        </p>
        
        <p style="margin-bottom: 30px;">
            Le invitamos a visitar nuestra página para acceder sin restricciones a todas las herramientas y recursos mejorados:
        </p>
        
        <div style="text-align: center; margin: 30px 0;">
            <a href="{enlace_ibexia}" target="_blank" style="
                display: inline-block; 
                padding: 12px 25px; 
                background-color: #007bff; 
                color: #ffffff; 
                text-decoration: none; 
                border-radius: 5px; 
                font-size: 16px; 
                font-weight: bold;
                border: 1px solid #007bff;
            ">
                Acceder a IBEXIA.es
            </a>
        </div>
        
        <p style="margin-top: 20px;">
            Agradecemos sinceramente su participación y apoyo continuo como usuario de IBEXIA.es.
        </p>

        <p style="font-size: 0.9em; color: #6c757d; margin-top: 20px;">
            Para cualquier consulta o aclaración respecto a esta transición, no dude en contactar con nuestro equipo de soporte.
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
        print(f"✅ Correo de Aviso de Servicio enviado a {destinatario_usuario} (Nombre: {nombre_usuario})")
        
    except Exception as e:
        print(f"❌ Error al enviar el correo a {destinatario_usuario}: {e}")

# ======================================================================
# 3. LÓGICA PRINCIPAL SIMPLIFICADA
# ======================================================================
def enviar_saludos_a_usuarios():
    try:
        print("Iniciando lectura de usuarios y envío de correos de Aviso de Servicio...")
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

        print("\nProceso de envío de Avisos de Servicio completado.")

    except Exception as e:
        print(f"❌ Error al ejecutar el script principal de Avisos de Servicio: {e}")

if __name__ == '__main__':
    enviar_saludos_a_usuarios()
