from datetime import datetime, timedelta
import pytz
import os
from app.config import Config
from app.common.mail import send_mail, sendMailOffice365, sendMailOffice365
from app.common.s3_utils import *

def send_email_with_results(s3_key, metricas, period):
    download_link = generate_s3_download_link(s3_key, expiration_hours=12)
    if not download_link:
        print(f"[ALERTA] No se pudo generar enlace de descarga para {s3_key}")
        return
    
    filename = os.path.basename(s3_key)
    file_size = get_s3_file_size(s3_key)
    
    hora_actual = datetime.now(pytz.timezone("America/Lima")).strftime('%H:%M:%S')
    anio_actual = datetime.now(pytz.timezone("America/Lima")).strftime('%Y')

    ##current_time = (datetime.now(pytz.timezone("America/Lima")) - timedelta(days=1)).strftime('%Y-%m-%d')
    subject = f'Conciliacion Yape  {period}'
    recipients = Config.CORREO_YAPE.split(',')
    mensaje_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
    </head>
    <body style="font-family: Arial, sans-serif; margin: 0; padding: 0; color: #333;">
        <!-- Encabezado -->
        <table width="100%" cellpadding="0" cellspacing="0" bgcolor="#640E76">
            <tr>
                <td align="center" style="padding: 15px;">
                    <h1 style="color: white; margin: 0;">Reporte de Conciliacion Yape vs Calimaco</h1>
                    <p style="color: white; margin: 5px 0 0;">Resumen de operaciones - Fecha {period}</p>
                </td>
            </tr>
        </table>
        
        <!-- Contenido principal -->
        <table width="100%" cellpadding="20" cellspacing="0" style="border: 1px solid #ddd; border-top: none;">
            <tr>
                <td>
                    <!-- Seccion Datos Analizados -->
                    <table width="100%" cellpadding="15" cellspacing="0" bgcolor="#f9f9f9" style="margin-bottom: 15px; border-left: 4px solid #640e76; border-radius: 5px;">
                        <tr>
                            <td>
                                <h2 style="color: #2C3E50; margin-top: 0; border-bottom: 2px solid #f0f0f0; padding-bottom: 10px;">Datos Analizados</h2>
                                <p><strong style="color: #2c3e50;">Total registros Calimaco:</strong> <span style="color: #2C3E50; font-weight: bold;">{metricas['total_calimaco']}</span></p>
                                <p><strong style="color: #2c3e50;">Total registros Yape:</strong> <span style="color: #2C3E50; font-weight: bold;">{metricas['total_yape']}</span></p>
                            </td>
                        </tr>
                    </table>
                    
                    <!-- Seccion Registros Aprobados -->
                    <table width="100%" cellpadding="15" cellspacing="0" bgcolor="#f9f9f9" style="margin-bottom: 15px; border-left: 4px solid #640e76; border-radius: 5px;">
                        <tr>
                            <td>
                                <h2 style="color: #2C3E50; margin-top: 0; border-bottom: 2px solid #f0f0f0; padding-bottom: 10px;">Registros Aprobados</h2>
                                <p><strong style="color: #2c3e50;">Calimaco:</strong> <span style="color: #2C3E50; font-weight: bold;">{metricas['aprobados_calimaco']}</span></p>
                                <p><strong style="color: #2c3e50;">Yape:</strong> <span style="color: #2C3E50; font-weight: bold;">{metricas['aprobados_yape']}</span></p>
                            </td>
                        </tr>
                    </table>
                    
                    <!-- Seccion Recaudacion (destacada) -->
                    <table width="100%" cellpadding="15" cellspacing="0" bgcolor="#e8f4fd" style="margin-bottom: 15px; border-left: 4px solid #640e76; border-radius: 5px;">
                        <tr>
                            <td>
                                <h2 style="color: #2C3E50; margin-top: 0;">Recaudacion Total</h2>
                                <p><strong style="color: #2c3e50;">Calimaco:</strong> <span style="color: #2C3E50; font-weight: bold;">S/. {metricas['recaudacion_calimaco']}</span></p>
                                <p><strong style="color: #2c3e50;">Yape:</strong> <span style="color: #2C3E50; font-weight: bold;">S/. {metricas['recaudacion_yape']}</span></p>
                            </td>
                        </tr>
                    </table>
                    
                    <!-- Seccion Conciliacion Final -->
                    <table width="100%" cellpadding="15" cellspacing="0" bgcolor="#eafaf1" style="margin-bottom: 15px; border-left: 4px solid #27ae60; border-radius: 5px;">
                        <tr>
                            <td>
                                <h2 style="color: #27ae60; margin-top: 0; border-bottom: 2px solid #f0f0f0; padding-bottom: 10px;">Conciliacion Final</h2>
                                <p><strong style="color: #27ae60;">Registros conciliados:</strong> <span style="color: #27ae60; font-weight: bold;">{metricas['conciliados_total']}</span></p>
                                <p><strong style="color: #27ae60;">Monto Calimaco:</strong> <span style="color: #27ae60; font-weight: bold;">S/. {metricas['conciliados_monto_calimaco']}</span></p>
                                <p><strong style="color: #27ae60;">Monto Yape:</strong> <span style="color: #27ae60; font-weight: bold;">S/. {metricas['conciliados_monto_yape']}</span></p>
                            </td>
                        </tr>
                    </table>

                    <!-- Seccion No Conciliados -->
                    <table width="100%" cellpadding="15" cellspacing="0" bgcolor="#fdeaea" style="margin-bottom: 15px; border-left: 4px solid #e74c3c; border-radius: 5px;">
                        <tr>
                            <td>
                                <h2 style="color: #e74c3c; margin-top: 0; border-bottom: 2px solid #f0f0f0; padding-bottom: 10px;">No Conciliados</h2>
                                <p><strong style="color: #e74c3c;">Calimaco:</strong> <span style="color: #e74c3c; font-weight: bold;">{metricas['no_conciliados_calimaco']}</span></p>
                                <p><strong style="color: #e74c3c;">Yape:</strong> <span style="color: #e74c3c; font-weight: bold;">{metricas['no_conciliados_yape']}</span></p>
                            </td>
                        </tr>
                    </table>
                    
                    <!-- Seccion Descarga de Archivo -->
                    <table width="100%" cellpadding="20" cellspacing="0" bgcolor="#fff3cd" style="margin-bottom: 20px; border-left: 4px solid #ffc107; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                        <tr>
                            <td align="center">
                                <h2 style="color: #856404; margin-top: 0; margin-bottom: 15px;">Descargar Reporte Detallado</h2>
                                <p style="color: #856404; margin: 0 0 5px 0;"><strong>Archivo:</strong> {filename}</p>
                                <p style="color: #856404; margin: 0 0 20px 0;"><strong>Tamaño:</strong> {file_size:.2f} MB</p>
                                <div style="margin: 20px 0;">
                                    <a href="{download_link}" 
                                       style="color : #0F6CBD">
                                        DESCARGAR ARCHIVO
                                    </a>
                                </div>
                                <p style="color: #856404; font-size: 14px; margin: 10px 0 0 0; font-style: italic;">
                                    Este enlace es valido por 12 horas
                                </p>
                            </td>
                        </tr>
                    </table>

                    <!-- Pie de pagina -->
                    <table width="100%" cellpadding="0" cellspacing="0">
                        <tr>
                            <td align="center" style="padding-top: 30px; color: #777; font-size: 0.9em;">
                                <p>Este reporte fue generado automaticamente - {hora_actual}</p>
                                <p>En caso de discrepancias, revisar el sistema recaudador.</p>
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


def send_email_with_results_month(s3_key, metricas, period):
    download_link = generate_s3_download_link(s3_key, expiration_hours=12)
    if not download_link:
        print(f"[ALERTA] No se pudo generar enlace de descarga para {s3_key}")
        return
    
    filename = os.path.basename(s3_key)
    file_size = get_s3_file_size(s3_key)

    hora_actual = datetime.now(pytz.timezone("America/Lima")).strftime('%H:%M:%S')
    anio_actual = datetime.now(pytz.timezone("America/Lima")).strftime('%Y')
    
    subject = f'Conciliacion Yape para el periodo {period}'
    recipients = Config.CORREO_YAPE.split(',')
    mensaje_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
    </head>
    <body style="font-family: Arial, sans-serif; margin: 0; padding: 0; color: #333;">
        <!-- Encabezado -->
        <table width="100%" cellpadding="0" cellspacing="0" bgcolor="#640E76">
            <tr>
                <td align="center" style="padding: 15px;">
                    <h1 style="color: white; margin: 0;">Reporte de Conciliacion Yape vs Calimaco</h1>
                    <p style="color: white; margin: 5px 0 0;">Resumen de operaciones - Periodo {period}</p>
                </td>
            </tr>
        </table>
        
        <!-- Contenido principal -->
        <table width="100%" cellpadding="20" cellspacing="0" style="border: 1px solid #ddd; border-top: none;">
            <tr>
                <td>
                    <!-- Seccion Datos Analizados -->
                    <table width="100%" cellpadding="15" cellspacing="0" bgcolor="#f9f9f9" style="margin-bottom: 15px; border-left: 4px solid #640e76; border-radius: 5px;">
                        <tr>
                            <td>
                                <h2 style="color: #2C3E50; margin-top: 0; border-bottom: 2px solid #f0f0f0; padding-bottom: 10px;">Datos Analizados</h2>
                                <p><strong style="color: #2c3e50;">Total registros Calimaco:</strong> <span style="color: #2C3E50; font-weight: bold;">{metricas['total_calimaco']}</span></p>
                                <p><strong style="color: #2c3e50;">Total registros Yape:</strong> <span style="color: #2C3E50; font-weight: bold;">{metricas['total_yape']}</span></p>
                            </td>
                        </tr>
                    </table>
                    
                    <!-- Seccion Registros Aprobados -->
                    <table width="100%" cellpadding="15" cellspacing="0" bgcolor="#f9f9f9" style="margin-bottom: 15px; border-left: 4px solid #640e76; border-radius: 5px;">
                        <tr>
                            <td>
                                <h2 style="color: #2C3E50; margin-top: 0; border-bottom: 2px solid #f0f0f0; padding-bottom: 10px;">Registros Aprobados</h2>
                                <p><strong style="color: #2c3e50;">Calimaco:</strong> <span style="color: #2C3E50; font-weight: bold;">{metricas['aprobados_calimaco']}</span></p>
                                <p><strong style="color: #2c3e50;">Yape:</strong> <span style="color: #2C3E50; font-weight: bold;">{metricas['aprobados_yape']}</span></p>
                            </td>
                        </tr>
                    </table>
                    
                    <!-- Seccion Recaudacion (destacada) -->
                    <table width="100%" cellpadding="15" cellspacing="0" bgcolor="#e8f4fd" style="margin-bottom: 15px; border-left: 4px solid #640e76; border-radius: 5px;">
                        <tr>
                            <td>
                                <h2 style="color: #2C3E50; margin-top: 0;">Recaudacion Total</h2>
                                <p><strong style="color: #2c3e50;">Calimaco:</strong> <span style="color: #2C3E50; font-weight: bold;">S/. {metricas['recaudacion_calimaco']}</span></p>
                                <p><strong style="color: #2c3e50;">Yape:</strong> <span style="color: #2C3E50; font-weight: bold;">S/. {metricas['recaudacion_yape']}</span></p>
                            </td>
                        </tr>
                    </table>
                    
                    <!-- Seccion Conciliacion Final -->
                    <table width="100%" cellpadding="15" cellspacing="0" bgcolor="#eafaf1" style="margin-bottom: 15px; border-left: 4px solid #27ae60; border-radius: 5px;">
                        <tr>
                            <td>
                                <h2 style="color: #27ae60; margin-top: 0; border-bottom: 2px solid #f0f0f0; padding-bottom: 10px;">Conciliacion Final</h2>
                                <p><strong style="color: #27ae60;">Registros conciliados:</strong> <span style="color: #27ae60; font-weight: bold;">{metricas['conciliados_total']}</span></p>
                                <p><strong style="color: #27ae60;">Monto Calimaco:</strong> <span style="color: #27ae60; font-weight: bold;">S/. {metricas['conciliados_monto_calimaco']}</span></p>
                                <p><strong style="color: #27ae60;">Monto Yape:</strong> <span style="color: #27ae60; font-weight: bold;">S/. {metricas['conciliados_monto_yape']}</span></p>
                            </td>
                        </tr>
                    </table>

                    <!-- Seccion No Conciliados -->
                    <table width="100%" cellpadding="15" cellspacing="0" bgcolor="#fdeaea" style="margin-bottom: 15px; border-left: 4px solid #e74c3c; border-radius: 5px;">
                        <tr>
                            <td>
                                <h2 style="color: #e74c3c; margin-top: 0; border-bottom: 2px solid #f0f0f0; padding-bottom: 10px;">No Conciliados</h2>
                                <p><strong style="color: #e74c3c;">Calimaco:</strong> <span style="color: #e74c3c; font-weight: bold;">{metricas['no_conciliados_calimaco']}</span></p>
                                <p><strong style="color: #e74c3c;">Yape:</strong> <span style="color: #e74c3c; font-weight: bold;">{metricas['no_conciliados_yape']}</span></p>
                            </td>
                        </tr>
                    </table>
                    
                    <!-- Seccion Descarga de Archivo -->
                    <table width="100%" cellpadding="20" cellspacing="0" bgcolor="#fff3cd" style="margin-bottom: 20px; border-left: 4px solid #ffc107; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                        <tr>
                            <td align="center">
                                <h2 style="color: #856404; margin-top: 0; margin-bottom: 15px;">Descargar Reporte Detallado</h2>
                                <p style="color: #856404; margin: 0 0 5px 0;"><strong>Archivo:</strong> {filename}</p>
                                <p style="color: #856404; margin: 0 0 20px 0;"><strong>Tamaño:</strong> {file_size:.2f} MB</p>
                                <div style="margin: 20px 0;">
                                    <a href="{download_link}" 
                                       style="color : #0F6CBD">
                                        DESCARGAR ARCHIVO
                                    </a>
                                </div>
                                <p style="color: #856404; font-size: 14px; margin: 10px 0 0 0; font-style: italic;">
                                    Este enlace es valido por 12 horas
                                </p>
                            </td>
                        </tr>
                    </table>

                    <!-- Pie de pagina -->
                    <table width="100%" cellpadding="0" cellspacing="0">
                        <tr>
                            <td align="center" style="padding-top: 30px; color: #777; font-size: 0.9em;">
                                <p>Este reporte fue generado automaticamente - {hora_actual}</p>
                                <p>En caso de discrepancias, revisar el sistema recaudador.</p>
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

    # enviar correo con office365
    sendMailOffice365(Config.SMTP_USER, subject, mensaje_html, recipients, None)



if __name__ == "__main__":
    # Datos ficticios
    s3_key = "digital/apps/total-secure/conciliaciones/processed/Kashio_Conciliacion_Ventas_20260107152404.xlsx"
    metricas = {
        "total_calimaco": 100,
        "total_yape": 120,
        "aprobados_calimaco": 80,
        "aprobados_yape": 90,
        "recaudacion_calimaco": 1500.50,
        "recaudacion_yape": 1600.75,
        "conciliados_total": 70,
        "conciliados_monto_calimaco": 1200.00,
        "conciliados_monto_yape": 1250.00,
        "no_conciliados_calimaco": 30,
        "no_conciliados_yape": 40
    }
    period = "2025-08-24"

    send_email_with_results(s3_key, metricas, period)
