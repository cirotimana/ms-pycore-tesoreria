from datetime import datetime
import pytz
import os
from app.config import Config
from app.common.mail import send_mail, sendMailOffice365

def send_email_with_results(df, total_ip, output_dir):
    """Envia un correo con los resultados del analisis"""
    current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d_%H%M') + '00'
    
    # generar archivo CSV
    csv_file_path = os.path.join(output_dir, f'ConcentradorIP_Registros_{current_time}.csv')
    df.to_csv(csv_file_path, index=False)
    
    # preparar datos para el correo
    total_registros = len(df)
    total_ips_unicas = df['ip'].nunique()
    df_ips_registros = df.groupby('ip').size().reset_index(name='registros').sort_values(by='registros', ascending=False)
    
    # generar tabla HTML manualmente con la columna de nivel de riesgo
    html_table_rows = ""
    for _, row in df_ips_registros.iterrows():
        ip = row['ip']
        registros = row['registros']

        # nivel de riesgo
        if registros >= 20:
            nivel_riesgo = "🔴 ALTO"
        elif registros >= 10:
            nivel_riesgo = "🟡 MEDIO"
        else:
            nivel_riesgo = "🟢 BAJO"
        
        html_table_rows += f"""
        <tr>
            <td style="padding: 10px 15px; border-bottom: 1px solid #e0e0e0; text-align: left;">{ip}</td>
            <td style="padding: 10px 15px; border-bottom: 1px solid #e0e0e0; text-align: center; font-weight: bold;">{registros}</td>
            <td style="padding: 10px 15px; border-bottom: 1px solid #e0e0e0; text-align: center;">{nivel_riesgo}</td>
        </tr>
        """

    anio_actual = datetime.now(pytz.timezone("America/Lima")).strftime('%Y')
    hora_actual = datetime.now(pytz.timezone("America/Lima")).strftime('%H:%M:%S')
    
    # construir mensaje
    subject = f'Concentracion IP - Registros {current_time}'
    recipients = Config.CORREO_CONCENTRACIONIP.split(',')
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
                        <h1 style="color: #FFFFFF; margin: 0; font-size: 24px;">Reporte de Concentración de IP</h1>
                        <p style="color: #FFFFFF; margin: 10px 0 0;">Detección de posibles actividades inusuales - Últimos 3 días</p>
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
                                        Se envía la información de concentración de IP de los registros digitales de los últimos 3 días 
                                        que presenten más de 5 registros por dirección IP, lo cual puede indicar actividad inusual 
                                        que requiere atención.
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
                                                <div style="font-size: 32px; font-weight: bold; color: #FF0000;">{total_ip}</div>
                                                <div style="color: #000000; font-size: 14px; margin-top: 5px;">TOTAL REGISTROS ANALIZADOS</div>
                                            </td>
                                        </tr>
                                    </table>
                                </td>
                                <td width="33%" valign="top" style="padding: 0 10px;">
                                    <table width="100%" cellpadding="15" cellspacing="0" bgcolor="#f9f9f9" style="border-left: 4px solid #FF0000; border-radius: 4px;">
                                        <tr>
                                            <td align="center">
                                                <div style="font-size: 32px; font-weight: bold; color: #FF0000;">{total_ips_unicas}</div>
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
                                                <div style="color: #000000; font-size: 14px; margin-top: 5px;">CONCENTRACIONES POR IP</div>
                                            </td>
                                        </tr>
                                    </table>
                                </td>
                            </tr>
                        </table>
                        
                        <!-- Tabla de detalles -->
                        <table width="100%" cellpadding="0" cellspacing="0" style="margin-top: 30px;">
                            <tr>
                                <td>
                                    <h2 style="color: #FF0000; margin-top: 0; border-bottom: 2px solid #f0f0f0; padding-bottom: 10px;">Detalle de Concentraciones por IP</h2>
                                    
                                    <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse: collapse; margin-top: 15px; border: 1px solid #d0d0d0;">
                                        <tr>
                                            <th style="background-color: #FF0000; color: #FFFFFF; padding: 12px 15px; text-align: left; border: 1px solid #d0d0d0;">Dirección IP</th>
                                            <th style="background-color: #FF0000; color: #FFFFFF; padding: 12px 15px; text-align: center; border: 1px solid #d0d0d0;">Cantidad de Registros</th>
                                            <th style="background-color: #FF0000; color: #FFFFFF; padding: 12px 15px; text-align: center; border: 1px solid #d0d0d0;">Nivel de Riesgo</th>
                                        </tr>
                                        {html_table_rows}
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
                                        Este reporte incluye un archivo CSV adjunto con el detalle completo con las ips detectadas en el período de 72 horas para su análisis detallado.
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
                                        <li>Verificar la legitimidad de las IPs con mayor número de registros</li>
                                        <li>Analizar patrones de comportamiento inusual</li>
                                        <li>Considerar implementar medidas de seguridad adicionales para IPs sospechosas</li>
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


def send_empty_results_email(total_ip):
    """Envia un correo cuando no hay resultados"""
    current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d_%H%M') + '00'
    subject = f'Concentracion IP - Registros {current_time}'
    recipients = Config.CORREO_CONCENTRACIONIP.split(',')
    mensaje_html = f"""
        <html><body>
        <p>No se encontraron concentraciones de IP en los ultimos 3 dias con mas de 5 registros por IP.</p>
        <p>Total de registros analizados: {total_ip}</p>
        </body></html>
    """
    # send_mail(subject, mensaje_html, recipients)
    sendMailOffice365(Config.SMTP_USER, subject, mensaje_html, recipients)
    
