import os
import base64
import smtplib
import requests
from app.config import Config
from msal import ConfidentialClientApplication
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText


def send_mail_office_365(sender, subject, message_body, recipients, attachments=None):
    try:
        # configuracion
        client_id = Config.GRAPH_CLIENT_ID
        client_secret = Config.GRAPH_CLIENT_SECRET
        tenant_id = Config.GRAPH_TENANT_ID
        sender_email = sender
        
        if not all([client_id, client_secret, tenant_id, sender_email]):
            raise Exception("configuracion de graph api incompleta para envio de correos")
        
        # autenticacion
        app = ConfidentialClientApplication(
            client_id=client_id,
            client_credential=client_secret,
            authority=f"https://login.microsoftonline.com/{tenant_id}"
        )
        
        result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
        
        if "access_token" not in result:
            raise Exception(f"error obteniendo token: {result.get('error_description')}")
        
        token = result["access_token"]
        
        # preparar destinatarios
        to_recipients = [{"emailAddress": {"address": email.strip()}} for email in recipients]
        
        # preparar adjuntos
        prepared_attachments = []
        if attachments:
            for attachment in attachments:
                if isinstance(attachment, tuple):
                    # (filename, content)
                    filename, content = attachment
                    if isinstance(content, str):
                        content = content.encode('utf-8')
                    attachment_data = {
                        "@odata.type": "#microsoft.graph.fileAttachment",
                        "name": filename,
                        "contentBytes": base64.b64encode(content).decode('utf-8')
                    }
                    prepared_attachments.append(attachment_data)
                else:
                    # ruta de archivo
                    with open(attachment, 'rb') as f:
                        content = f.read()
                    attachment_data = {
                        "@odata.type": "#microsoft.graph.fileAttachment",
                        "name": os.path.basename(attachment),
                        "contentBytes": base64.b64encode(content).decode('utf-8')
                    }
                    prepared_attachments.append(attachment_data)
        
        # preparar mensaje
        message = {
            "subject": subject,
            "body": {
                "contentType": "HTML",
                "content": message_body
            },
            "toRecipients": to_recipients
        }
        
        if prepared_attachments:
            message["attachments"] = prepared_attachments
        
        # enviar correo
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        
        url = f"https://graph.microsoft.com/v1.0/users/{sender_email}/sendMail"
        payload = {"message": message}
        
        response = requests.post(url, headers=headers, json=payload)
        
        if response.status_code == 202:
            print(f"[ok] correo enviado exitosamente a: {', '.join(recipients)}")
            return True
        else:
            raise Exception(f"error enviando correo: {response.status_code} - {response.text}")
            
    except Exception as e:
        print(f"[error] error en send_mail_office_365: {str(e)}")
        return False
