from datetime import datetime, timedelta
import pytz
import os
from app.config import Config
from app.common.mail import send_mail, sendMailOffice365
from app.common.s3_utils import *

def send_liquidation_email(s3_key, metricas, period):
    download_link = generate_s3_download_link(s3_key, expiration_hours=12)
    if not download_link:
        print(f"[ALERTA] No se pudo generar enlace de descarga para {s3_key}")
        return
    
    filename = os.path.basename(s3_key)
    file_size = get_s3_file_size(s3_key)

    hora_actual = datetime.now(pytz.timezone("America/Lima")).strftime('%H:%M:%S')
    anio_actual = datetime.now(pytz.timezone("America/Lima")).strftime('%Y')

    # Determinar el estado y colores
    if metricas['resultado'] == 'CONCILIACION EXITOSA':
        estado_color = "#27ae60"
        estado_titulo = "La conciliacion se realizo de forma exitosa; los montos entre lo declarado y la liquidacion coinciden. "
        estado_fondo = "#eafaf1"
        borde_color = "#27ae60"
    else:
        estado_color = "#e74c3c"
        estado_titulo = "La conciliacion presenta detalles; los montos entre lo declarado y la liquidacion presentan discrepancias."
        estado_fondo = "#fdeaea"
        borde_color = "#e74c3c"
    
    referencias_html = "".join([f"<li>{ref}</li>" for ref in metricas['referencias']])

    subject = f'Liquidacion Kashio {period}'
    recipients = Config.CORREO_KASHIO_LIQ.split(',')
    mensaje_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
    </head>
    <body style="font-family: Arial, sans-serif; margin: 0; padding: 0; color: #333;">
        <!-- Encabezado -->
        <table width="100%" cellpadding="0" cellspacing="0" bgcolor="#0666eb">
            <tr>
                <td align="center" style="padding: 15px;">
                    <h1 style="color: white; margin: 0;">Reporte de Liquidacion Kashio</h1>
                    <p style="color: white; margin: 5px 0 0;">Conciliacion de Liquidaciones - Fecha {period}</p>
                </td>
            </tr>
        </table>
        
        <!-- Contenido principal -->
        <table width="100%" cellpadding="20" cellspacing="0" style="border: 1px solid #ddd; border-top: none;">
            <tr>
                <td>
                    
                    <!-- Seccion Montos Credito -->
                    <table width="100%" cellpadding="15" cellspacing="0" bgcolor="#f9f9f9" style="margin-bottom: 15px; border-left: 4px solid #28a745; border-radius: 5px;">
                        <tr>
                            <td>
                                <h2 style="color: #2C3E50; margin-top: 0; border-bottom: 2px solid #f0f0f0; padding-bottom: 10px;">Montos Credito</h2>
                                <p><strong style="color: #2c3e50;">Total Credito Kashio:</strong> <span style="color: #28a745; font-weight: bold;">S/. {round(metricas['total_credito_kashio'],2)}</span></p>
                                <p><strong style="color: #2c3e50;">Total Credito Liquidacion:</strong> <span style="color: #28a745; font-weight: bold;">S/. {round(metricas['total_credito_liquidacion'],2)}</span></p>
                            </td>
                        </tr>
                    </table>
                    
                    <!-- Seccion Montos Debito -->
                    <table width="100%" cellpadding="15" cellspacing="0" bgcolor="#f9f9f9" style="margin-bottom: 15px; border-left: 4px solid #dc3545; border-radius: 5px;">
                        <tr>
                            <td>
                                <h2 style="color: #2C3E50; margin-top: 0; border-bottom: 2px solid #f0f0f0; padding-bottom: 10px;">Montos Debito</h2>
                                <p><strong style="color: #2c3e50;">Total Debito Kashio:</strong> <span style="color: #dc3545; font-weight: bold;">S/. {round(metricas['total_debito_kashio'],2)}</span></p>
                                <p><strong style="color: #2c3e50;">Total Debito Liquidacion:</strong> <span style="color: #dc3545; font-weight: bold;">S/. {round(metricas['total_debito_liquidacion'],2)}</span></p>
                            </td>
                        </tr>
                    </table>
                    
                    <!-- Seccion Montos Netos -->
                    <table width="100%" cellpadding="15" cellspacing="0" bgcolor="#e8f4fd" style="margin-bottom: 15px; border-left: 4px solid #007bff; border-radius: 5px;">
                        <tr>
                            <td>
                                <h2 style="color: #2C3E50; margin-top: 0; border-bottom: 2px solid #f0f0f0; padding-bottom: 10px;">Montos Netos</h2>
                                <p><strong style="color: #2c3e50;">Total Neto Kashio:</strong> <span style="color: #007bff; font-weight: bold;">S/. {round(metricas['total_neto_kashio'],2)}</span></p>
                                <p><strong style="color: #2c3e50;">Total Neto Liquidacion:</strong> <span style="color: #007bff; font-weight: bold;">S/. {round(metricas['total_neto_liq'],2)}</span></p>
                            </td>
                        </tr>
                    </table>
                    
                    <!-- Seccion Resumen Comparativo -->
                    <table width="100%" cellpadding="15" cellspacing="0" bgcolor="#fff3cd" style="margin-bottom: 20px; border-left: 4px solid #ffc107; border-radius: 5px;">
                        <tr>
                            <td>
                                <h2 style="color: #856404; margin-top: 0; border-bottom: 2px solid #f0f0f0; padding-bottom: 10px;">Diferencias</h2>
                                <p><strong style="color: #856404;">Diferencia Credito:</strong> <span style="color: #856404; font-weight: bold;">S/. {round(metricas['total_credito_kashio'] - metricas['total_credito_liquidacion'],2)}</span></p>
                                <p><strong style="color: #856404;">Diferencia Debito:</strong> <span style="color: #856404; font-weight: bold;">S/. {round(metricas['total_debito_kashio'] - metricas['total_debito_liquidacion'],2)}</span></p>
                                <p><strong style="color: #856404;">Diferencia Neto:</strong> <span style="color: #856404; font-weight: bold;">S/. {round(metricas['total_neto_kashio'] - metricas['total_neto_liq'],2)}</span></p>
                            </td>
                        </tr>
                    </table>
                    
                    <!-- Seccion Referencias -->
                    <table width="100%" cellpadding="15" cellspacing="0" bgcolor="#f0f8ff" style="margin-bottom: 20px; border-left: 4px solid #17a2b8; border-radius: 5px;">
                        <tr>
                            <td>
                                <h2 style="color: #0c5460; margin-top: 0; border-bottom: 2px solid #f0f0f0; padding-bottom: 10px;">Referencias Procesadas</h2>
                                <ul style="color: #0c5460; font-weight: bold; margin: 0; padding-left: 20px;">
                                    {referencias_html}
                                </ul>
                            </td>
                        </tr>
                    </table>
                    
                    <!-- Estado de Conciliacion -->
                    <table width="100%" cellpadding="15" cellspacing="0" bgcolor="{estado_fondo}" style="margin-bottom: 20px; border-left: 4px solid {borde_color}; border-radius: 6px;">
                        <tr>
                            <td align="center" style="padding: 12px;">
                                <p style="color: {estado_color}; margin: 0; font-size: 16px; font-weight: bold;">
                                    {estado_titulo}
                                </p>
                            </td>
                        </tr>
                    </table>
                    
                    <!-- Seccion Descarga de Archivo -->
                    <table width="100%" cellpadding="20" cellspacing="0" bgcolor="#f8f9fa" style="margin-bottom: 20px; border-left: 4px solid #6c757d; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                        <tr>
                            <td align="center">
                                <h2 style="color: #495057; margin-top: 0; margin-bottom: 15px;">Descargar Reporte Detallado</h2>
                                <p style="color: #495057; margin: 0 0 5px 0;"><strong>Archivo:</strong> {filename}</p>
                                <p style="color: #495057; margin: 0 0 20px 0;"><strong>Tamaño:</strong> {file_size:.2f} MB</p>
                                <div style="margin: 20px 0;">
                                    <a href="{download_link}" 
                                       style="color : #0F6CBD">
                                        DESCARGAR ARCHIVO
                                    </a>
                                </div>
                                <p style="color: #495057; font-size: 14px; margin: 10px 0 0 0; font-style: italic;">
                                    Este enlace es valido por 12 horas
                                </p>
                            </td>
                        </tr>
                    </table>
                    
                    <!-- Pie de pagina -->
                    <table width="100%" cellpadding="0" cellspacing="0">
                        <tr>
                            <td align="center" style="padding-top: 30px; color: #777; font-size: 0.9em;">
                                <p>Este reporte de liquidacion fue generado automaticamente - {hora_actual}</p>
                                <p style="margin: 10px 0 0; font-size: 12px; color: #666;">Prevencion de Fraude - Optimizacion Operativa<br>© {anio_actual}</p>
                            </td>
                        </tr>
                    </table>
                </td>
            </tr>
        </table>
    </body>
    </html>
    """

    # enviar correo con adjunto en memoria
    sendMailOffice365(Config.SMTP_USER, subject, mensaje_html, recipients, None)
    
# # Ejemplo de uso
# if __name__ == "__main__":
#     metricas = {
#         'total_credito_kashio': 422309.81,
#         'total_credito_liquidacion': 422309.81,
#         'total_debito_kashio': 15202.75,
#         'total_debito_liquidacion': 15202.75,
#         'total_neto_kashio': 407107.06,
#         'total_neto_liq': 407107.06,
#         'resultado': 'CONCILIACION EXITOSA'
#     }

#     send_liquidation_email('digital/collectors/kashio/liquidations/processed/Kashio_Liquidations_Processed_DIA2_202510011810.xlsx', metricas, '2024-01-15')
