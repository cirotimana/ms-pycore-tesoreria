import os
os.chdir('data')
import base64
import smtplib
import requests
from app.config import Config
from msal import ConfidentialClientApplication
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText


def send_mail(asunto, mensajeBody, destinatarios, archivos_adjuntos=None):    
    mensaje = MIMEMultipart('alternative')
    mensaje['From'] = Config.REMITENTE
    mensaje['To'] = ', '.join(destinatarios)
    mensaje['Subject'] = asunto

    mensaje.attach(MIMEText(mensajeBody, 'html'))

    # Adjuntar archivos desde memoria o ruta local
    if archivos_adjuntos:
        for archivo in archivos_adjuntos:
            if isinstance(archivo, tuple):
                # (filename, content)
                filename, content = archivo
                adjunto_mime = MIMEApplication(content)
                adjunto_mime.add_header('Content-Disposition', 'attachment', filename=filename)
                mensaje.attach(adjunto_mime)
            else:
                with open(archivo, 'rb') as adjunto:
                    adjunto_mime = MIMEApplication(adjunto.read())
                    adjunto_mime.add_header('Content-Disposition', 'attachment', filename=os.path.basename(archivo))
                    mensaje.attach(adjunto_mime)

    server = smtplib.SMTP(Config.SMTP_SERVER, Config.SMTP_PORT)
    server.starttls()
    server.login(Config.SMTP_USER, Config.SMTP_PASSWORD)
    mensaje_texto = mensaje.as_string()
    server.sendmail(Config.REMITENTE, destinatarios, mensaje_texto)
    server.quit()

def sendMailOffice365(remitente, asunto, mensajeBody, destinatarios, archivos_adjuntos=None):
    try:
        # Configuración
        client_id = Config.GRAPH_CLIENT_ID
        client_secret = Config.GRAPH_CLIENT_SECRET
        tenant_id = Config.GRAPH_TENANT_ID
        sender_email = remitente
        
        if not all([client_id, client_secret, tenant_id, sender_email]):
            raise Exception("Configuración de Graph API incompleta para envío de correos")
        
        # Autenticación
        app = ConfidentialClientApplication(
            client_id=client_id,
            client_credential=client_secret,
            authority=f"https://login.microsoftonline.com/{tenant_id}"
        )
        
        result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
        
        if "access_token" not in result:
            raise Exception(f"Error obteniendo token: {result.get('error_description')}")
        
        token = result["access_token"]
        
        # Preparar destinatarios
        to_recipients = [{"emailAddress": {"address": email.strip()}} for email in destinatarios]
        
        # Preparar adjuntos
        attachments = []
        if archivos_adjuntos:
            for archivo in archivos_adjuntos:
                if isinstance(archivo, tuple):
                    # (filename, content)
                    filename, content = archivo
                    if isinstance(content, str):
                        content = content.encode('utf-8')
                    attachment_data = {
                        "@odata.type": "#microsoft.graph.fileAttachment",
                        "name": filename,
                        "contentBytes": base64.b64encode(content).decode('utf-8')
                    }
                    attachments.append(attachment_data)
                else:
                    # Ruta de archivo
                    with open(archivo, 'rb') as f:
                        content = f.read()
                    attachment_data = {
                        "@odata.type": "#microsoft.graph.fileAttachment",
                        "name": os.path.basename(archivo),
                        "contentBytes": base64.b64encode(content).decode('utf-8')
                    }
                    attachments.append(attachment_data)
        
        # Preparar mensaje
        message = {
            "subject": asunto,
            "body": {
                "contentType": "HTML",
                "content": mensajeBody
            },
            "toRecipients": to_recipients
        }
        
        if attachments:
            message["attachments"] = attachments
        
        # Enviar correo
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        
        url = f"https://graph.microsoft.com/v1.0/users/{sender_email}/sendMail"
        payload = {"message": message}
        
        response = requests.post(url, headers=headers, json=payload)
        
        if response.status_code == 202:
            print(f"Correo enviado exitosamente a: {', '.join(destinatarios)}")
            return True
        else:
            raise Exception(f"Error enviando correo: {response.status_code} - {response.text}")
            
    except Exception as e:
        print(f"Error en sendMailOffice365: {str(e)}")
        return False
