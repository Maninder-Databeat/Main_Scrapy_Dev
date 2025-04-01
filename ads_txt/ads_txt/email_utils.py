import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from scrapy.utils.project import get_project_settings

def send_email(subject, body):
    """Send an email using Gmail SMTP"""
    settings = get_project_settings()
    
    sender_email = settings.get('MAIL_FROM')
    receiver_email = settings.get('MAIL_TO')
    password = settings.get('MAIL_PASS')

    # Email message setup
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    # Send email via SMTP
    try:
        server = smtplib.SMTP(settings.get('MAIL_HOST'), settings.get('MAIL_PORT'))
        server.starttls()
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, msg.as_string())
        server.quit()
        print(f"Email sent: {subject}")
    except Exception as e:
        print(f"Error sending email: {e}")
