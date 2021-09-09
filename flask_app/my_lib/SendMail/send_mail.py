import os, sys
import traceback

import flask_app.settings.LogDeafultConfig

reader_path = os.path.dirname(os.path.abspath(__file__))
my_lib_path = os.path.dirname(reader_path)
project_path = os.path.dirname(my_lib_path)
sys.path.append(project_path)
from flask_app.settings import initial_settings as init
from email.mime.image import MIMEImage
import codecs

script_path = os.path.dirname(os.path.abspath(__file__))
im_path = init.REPORTS_REPO
template_path = os.path.join(init.TEMPLATES_REPO, "templates")
html_error_template_path = os.path.join(template_path, "reportar_error.html")
log_path = os.path.join(init.project_path, "logs")
log = flask_app.settings.LogDeafultConfig.LogDefaultConfig("mail.log").logger


def report_error(descripcion: str, detalle: str, from_email: str, emails, log_file_path):
    if isinstance(emails, str):
        recipients = [str(e).strip() for e in emails.split(";")]
    else:
        recipients = emails
    html_str = codecs.open(html_error_template_path, 'r', 'utf-8').read()
    html_str = html_str.replace("#descripción", descripcion)
    html_str = html_str.replace("#ERROR", detalle)
    subject = "[ERROR] Problema al enviar el REPORTE SISTEMAS AUXILIARES"
    success, msg = send_mail(html_str, subject, recipients, from_email, image_list=None, files=[log_file_path])
    if success:
        log.info(msg)
    else:
        log.error(msg)


def send_mail(msg_to_send: str, subject, recipients: list, from_email, image_list: list = None, files=None):
    from email.mime.multipart import MIMEMultipart
    from email.mime.application import MIMEApplication
    from email.mime.text import MIMEText
    from os.path import basename
    import smtplib

    try:
        im_to_append = list()
        # configure images if is needed
        if image_list is not None and isinstance(image_list, list):
            # This assumes the images are in "templates" folder
            for ix, image in enumerate(image_list):
                if "/" in image:
                    image_l = image.replace("./", "")
                    image_l = image_l.split("/")
                    to_check = os.path.join(im_path, *image_l)
                else:
                    to_check = os.path.join(im_path, image)

                if os.path.exists(to_check):
                    # redefine src= in html file (cid:image1)
                    msg_to_send = msg_to_send.replace(image, f"cid:image{ix}")
                    im_to_append.append(to_check)

        # configuraciones generales:
        SERVER = "mail.cenace.org.ec"

        # create message object instance
        msg = MIMEMultipart('related')

        # setup the parameters of the message
        # password = "cenace.123"
        # recipients = ["mbautista@cenace.org.ec","ems@cenace.org.ec"]
        msg['From'] = from_email
        msg['To'] = ", ".join(recipients)
        msg['Subject'] = subject
        msg.preamble = """"""

        # add in the message body as HTML content
        HTML_BODY = MIMEText(msg_to_send, 'html')
        msg.attach(HTML_BODY)

        # adding messages to the mail (only the ones that where found)
        for ix, image in enumerate(im_to_append):
            try:
                fp = open(os.path.join(im_path, image), 'rb')
                msgImage = MIMEImage(fp.read())
                fp.close()
                # Define the image's ID as referenced above
                msgImage.add_header('Content-ID', f'<image{ix}>')
                msg.attach(msgImage)
            except:
                pass

        # Add files if is needed:
        for f in files or []:
            with open(f, "rb") as fil:
                part = MIMEApplication(
                    fil.read(),
                    Name=basename(f)
                )
            # After the file is closed
            part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
            msg.attach(part)

        # create server
        server = smtplib.SMTP(SERVER)

        server.starttls()

        # Login Credentials for sending the mail
        # server.login(msg['From'], password)

        # send the message via the server.
        server.sendmail(msg['From'], recipients, msg.as_string())

        server.quit()
        return True, f"Correo enviado correctamente. Detalles enviados a: {msg['To']}"
    except Exception as e:
        tb = traceback.format_exc()
        log.error(tb)
        return False, f"Error al enviar el correo electrónico: {str(e)}"
