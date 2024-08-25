from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from . import models
from django.shortcuts import redirect
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import os.path
from django.http import JsonResponse


def homepage_view(request):
    # assert False, request
    from ..admin import models as org
    from operator import itemgetter
    messages_data = models.get_messages()
    data = org.get_all_person()
    newlist = sorted(data, key=itemgetter('loc'), reverse=True)
    context_var = {
        'teamdata': newlist,
        'domains': org.get_all_domain(),
        'rates': org.get_all_rates(),
        'roles': org.get_all_roles(),
        'locations': org.get_all_locations()
    }
    return render(request, "homepage.html", context_var)


def contact(request):
    if request.POST.get('sendmail'):
        print("this")
    return render(request, "contact.html")


def about(request):
    return render(request, "about.html")


@login_required(login_url='/identity/')
def messages(request):
    messages_data = models.get_messages()

    if request.method == "POST":
        messages_data = models.set_messages(request.POST.get('bruce'), request.POST.get('sreeman'))
        return re


@login_required(login_url='/identity/')
def send_email(request):
    ret = {'success': False}

    if request.method == 'GET' and 'to' in request.GET:
        message = "testing 123"
        from_email = 'vn05672@delhaize.com'
        to_email = request.GET['to']
        subject = 'Testing from RFS Automation'
        send_mail(to_email, subject, message, '')

        ret['success'] = True

    return JsonResponse(ret, safe=False)


def send_mail(email_recipient,
               email_subject,
               email_message,
               attachment_location=''):
    email_sender = 'vn05672@delhaize.com'

    msg = MIMEMultipart()
    msg['From'] = email_sender
    msg['To'] = email_recipient
    msg['Subject'] = email_subject

    msg.attach(MIMEText(email_message, 'plain'))

    if attachment_location != '':
        filename = os.path.basename(attachment_location)
        attachment = open(attachment_location, "rb")
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition',
                        "attachment; filename= %s" % filename)
        msg.attach(part)

    server = smtplib.SMTP('smtp-us.delhaize.com')
    server.ehlo()
    server.starttls()
    # server.login('vn05672@delhaize.com', 'Silver66')
    text = msg.as_string()
    server.sendmail(email_sender, email_recipient, text)
    print('email sent')
    server.quit()
    print("SMPT server connection error")

    return True
