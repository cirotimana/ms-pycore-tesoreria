from datetime import datetime
import pytz
import os
from app.config import Config
from app.common.mail import send_mail, sendMailOffice365

def send_email_with_results(df, total_dni, output_dir):
    """Envia un correo con los resultados del analisis"""
    current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d_%H%M') + '00'
    
    # generar archivo csv
    csv_file_path = os.path.join(output_dir, f'DNICorrelativos_{current_time}.csv')
    df.to_csv(csv_file_path, index=False)
    
    # preparar datos para el correo
    total_registros = len(df)
    total_registros_unicos = df['case_number'].nunique()
    
    anio_actual = datetime.now(pytz.timezone("America/Lima")).strftime('%Y')
    hora_actual = datetime.now(pytz.timezone("America/Lima")).strftime('%H:%M:%S')

    # construir mensaje
    subject = f'DNI Correlativos {current_time}'
    recipients = Config.CORREO_DNICORRELATIVOS.split(',')
    mensaje_html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
        </head>
        <body style="font-family: Arial, sans-serif; margin: 0; padding: 0; color: #000000;">
            <!-- Encabezado -->
            <table width="100%" cellpadding="0" cellspacing="0" bgcolor="#FF0000">
                <tr>
                    <td align="center" style="padding: 20px;">
                        <h1 style="color: #FFFFFF; margin: 0; font-size: 24px;">Reporte de DNI Correlativos</h1>
                        <p style="color: #FFFFFF; margin: 10px 0 0;">Detección de posibles actividades inusuales - Últimas 48 horas</p>
                    </td>
                </tr>
            </table>
            
            <!-- Contenido principal -->
            <table width="100%" cellpadding="25" cellspacing="0" bgcolor="#FFFFFF" style="border: 1px solid #e0e0e0; border-top: none;">
                <tr>
                    <td>
                        <!-- Introducción -->
                        <table width="100%" cellpadding="0" cellspacing="0">
                            <tr>
                                <td style="padding-bottom: 20px;">
                                    <p style="margin: 0; color: #000000; line-height: 1.6;">
                                        Se envía la información de DNI correlativos de los registros de las últimas 48 horas,
                                        detectando posibles patrones inusuales que requieren atención y verificación.
                                    </p>
                                </td>
                            </tr>
                        </table>
                        
                        <!-- Estadísticas principales -->
                        <table width="100%" cellpadding="0" cellspacing="0">
                            <tr>
                                <td width="33%" valign="top" style="padding: 0 10px;">
                                    <table width="100%" cellpadding="15" cellspacing="0" bgcolor="#f9f9f9" style="border-left: 4px solid #FF0000; border-radius: 4px;">
                                        <tr>
                                            <td align="center">
                                                <div style="font-size: 32px; font-weight: bold; color: #FF0000;">{total_dni}</div>
                                                <div style="color: #000000; font-size: 14px; margin-top: 5px;">TOTAL REGISTROS ANALIZADOS</div>
                                            </td>
                                        </tr>
                                    </table>
                                </td>
                                <td width="33%" valign="top" style="padding: 0 10px;">
                                    <table width="100%" cellpadding="15" cellspacing="0" bgcolor="#f9f9f9" style="border-left: 4px solid #FF0000; border-radius: 4px;">
                                        <tr>
                                            <td align="center">
                                                <div style="font-size: 32px; font-weight: bold; color: #FF0000;">{total_registros_unicos}</div>
                                                <div style="color: #000000; font-size: 14px; margin-top: 5px;">CASOS DETECTADOS</div>
                                            </td>
                                        </tr>
                                    </table>
                                </td>
                                <td width="33%" valign="top" style="padding: 0 10px;">
                                    <table width="100%" cellpadding="15" cellspacing="0" bgcolor="#f9f9f9" style="border-left: 4px solid #FF0000; border-radius: 4px;">
                                        <tr>
                                            <td align="center">
                                                <div style="font-size: 32px; font-weight: bold; color: #FF0000;">{total_registros}</div>
                                                <div style="color: #000000; font-size: 14px; margin-top: 5px;">DNI DETECTADOS</div>
                                            </td>
                                        </tr>
                                    </table>
                                </td>
                                
                            </tr>
                        </table>
                        
                        <!-- Información adicional -->
                        <table width="100%" cellpadding="0" cellspacing="0" style="margin-top: 30px;">
                            <tr>
                                <td style="background-color: #f8f9fa; border-left: 4px solid #FF0000; border-radius: 4px; padding: 15px;">
                                    <h3 style="color: #FF0000; margin-top: 0;">Resumen del Reporte</h3>
                                    <p style="color: #000000; margin: 10px 0; line-height: 1.5;">
                                        Este reporte incluye un archivo CSV adjunto con el detalle completo de los 
                                        DNI correlativos detectados en el período de 48 horas para su análisis detallado.
                                    </p>
                                </td>
                            </tr>
                        </table>
                        
                        <!-- Notas importantes -->
                        <table width="100%" cellpadding="0" cellspacing="0" style="margin-top: 30px;">
                            <tr>
                                <td style="background-color: #fff8f8; border-left: 4px solid #FF0000; border-radius: 4px; padding: 15px;">
                                    <h3 style="color: #FF0000; margin-top: 0;">Acciones Recomendadas</h3>
                                    <ul style="color: #000000; margin: 10px 0; padding-left: 20px;">
                                        <li>Verificar la legitimidad de los DNI con mayor número de registros</li>
                                        <li>Analizar posibles intentos de suplantación de identidad</li>
                                        <li>Revisar patrones de comportamiento inusual en las transacciones</li>
                                    </ul>
                                </td>
                            </tr>
                        </table>
                        
                        <!-- Pie de página -->
                        <table width="100%" cellpadding="0" cellspacing="0" style="margin-top: 30px;">
                            <tr>
                                <td align="center" style="padding-top: 20px; color: #666666; font-size: 12px; border-top: 1px solid #e0e0e0;">
                                    <p>Reporte generado automáticamente - {hora_actual}</p>
                                    <p style="margin: 10px 0 0; font-size: 12px; color: #666;">Prevención de Fraude - Optimización Operativa<br>© {anio_actual}</p>
                                </td>
                            </tr>
                        </table>
                    </td>
                </tr>
            </table>
        </body>
        </html>
    """
    
    # enviar correo
    # send_mail(subject, mensaje_html, recipients, [csv_file_path])
    sendMailOffice365(Config.SMTP_USER, subject, mensaje_html, recipients, [csv_file_path])
    
    
def send_empty_results_email(total_dni):
    """envia un correo cuando no hay resultados"""
    current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d_%H%M') + '00'
    subject = f'DNI Correlativos {current_time}'
    recipients = Config.CORREO_DNICORRELATIVOS.split(',')
    mensaje_html = f"""
        <html><body>
        <p>No se encontraron DNI correlativos en las ultimas 48 horas.</p>
        <p>Total de registros analizados: {total_dni}</p>
        </body></html>
    """
    # send_mail(subject, mensaje_html, recipients)
    sendMailOffice365(Config.SMTP_USER, subject, mensaje_html, recipients)