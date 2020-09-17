import poplib
import smtplib
from email.header import decode_header
from email.mime.text import MIMEText
from email.parser import Parser
from email.utils import parseaddr


class SimEmail:
    def __init__(self, username, password, sender,mtpserver='smtp.qq.com', popServer="pop.qq.com"):
        self.username = username
        self.password = password
        self.sender = sender
        self.mtpserver = mtpserver
        self.popserver = popServer

    def sendMail(self, subject, text, receiver):
        smtpserver = self.mtpserver
        sender = self.sender
        username = self.username
        password = self.password
        msg = MIMEText(text, 'html', 'utf-8')
        msg['From'] = sender
        msg['To'] = receiver
        msg['Subject'] = subject

        smtp = smtplib.SMTP()
        smtp.connect(smtpserver)
        smtp.login(username, password)
        smtp.sendmail(sender, receiver, msg.as_string())
        smtp.quit()

    def getMail(self, index=None, isDelete=False):
        # 连接到POP3服务器:
        server = poplib.POP3_SSL(self.popserver)

        # 可以打开或关闭调试信息:
        # server.set_debuglevel(1)

        # 可选:打印POP3服务器的欢迎文字:
        # print(server.getwelcome())

        # 身份认证:
        server.user(self.username)
        server.pass_(self.password)

        # stat()返回邮件数量和占用空间:
        # print('Messages: %s. Size: %s' % server.stat())

        # list()返回所有邮件的编号:
        resp, mails, octets = server.list()

        # 可以查看返回的列表类似['1 82923', '2 2184', ...]
        # print(mails)

        # 获取最新一封邮件, 注意索引号从1开始:
        if index is None:
            index = len(mails)
        resp, lines, octets = server.retr(index)

        # lines存储了邮件的原始文本的每一行,
        # 可以获得整个邮件的原始文本:
        msg_content = bytes('\r\n', encoding='utf-8').join(lines)

        # 稍后解析出邮件:
        msg = Parser().parsestr(str(msg_content, encoding='utf-8'))

        result = self.print_info(msg)

        result["mail_list"] = mails

        # 可以根据邮件索引号直接从服务器删除邮件:
        if isDelete:
            server.dele(index)

        # 关闭连接:
        server.quit()
        return result

    # 猜测编码格式
    def guess_charset(self, msg):
        charset = msg.get_charset()
        if charset is None:
            content_type = msg.get('Content-Type', '').lower()
            pos = content_type.find('charset=')
            if pos >= 0:
                charset = content_type[pos + 8:].strip()
        return charset

    # 解码
    def decode_str(self, s):
        value, charset = decode_header(s)[0]
        if charset:
            value = value.decode(charset)
        return value

    # 解析邮件内容
    def print_info(self, msg, indent=0):
        result = {}
        if indent == 0:
            for header in ['From', 'To', 'Subject']:
                value = msg.get(header, '')
                if value:
                    if header == 'Subject':
                        value = self.decode_str(value)
                    else:
                        hdr, addr = parseaddr(value)
                        name = self.decode_str(hdr)
                        value = u'%s <%s>' % (name, addr)
                # print('%s%s: %s' % ('  ' * indent, header, value))
                result[header] = value
        if (msg.is_multipart()):
            parts = msg.get_payload()
            for n, part in enumerate(parts):
                # print('%spart %s' % ('  ' * indent, n))
                # print('%s--------------------' % ('  ' * indent))
                self.print_info(part, indent + 1)
        else:
            content_type = msg.get_content_type()
            if content_type == 'text/plain' or content_type == 'text/html':
                content = msg.get_payload(decode=True)
                charset = self.guess_charset(msg)
                if charset:
                    content = content.decode(charset)
                # print('%sText: %s' % ('  ' * indent, content + '...'))
                result["Content"] = content
            else:
                # print('%sAttachment: %s' % ('  ' * indent, content_type))
                result["Attachment"] = content_type
        return result


if __name__ == '__main__':
    pass