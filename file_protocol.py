import json
import logging
import shlex

from file_interface import FileInterface

"""
* class FileProtocol bertugas untuk memproses 
data yang masuk, dan menerjemahkannya apakah sesuai dengan
protokol/aturan yang dibuat

* data yang masuk dari client adalah dalam bentuk bytes yang 
pada akhirnya akan diproses dalam bentuk string

* class FileProtocol akan memproses data yang masuk dalam bentuk
string
"""



class FileProtocol:
    def __init__(self):
        self.file = FileInterface()
    def proses_string(self, string_datamasuk=''):
        logging.warning(f"string diproses: {repr(string_datamasuk)}")
        try:
            if '\r\n' in string_datamasuk:
                header, body = string_datamasuk.split('\r\n', 1)
            else:
                header, body = string_datamasuk, ''
            c = shlex.split(header.lower())
            c_request = c[0].strip()
            logging.warning(f"memproses request: {c_request}")
            params = c[1:]

            if c_request == 'upload':
                cl = getattr(self.file, c_request)([params[0], body.strip()])
            else:
                cl = getattr(self.file, c_request)(params)
            return json.dumps(cl)
        except Exception as e:
            logging.error(str(e))
            return json.dumps(dict(status='ERROR', data='request tidak dikenali'))



if __name__=='__main__':
    #contoh pemakaian
    fp = FileProtocol()
    print(fp.proses_string("LIST"))
    print(fp.proses_string("GET pokijan.jpg"))
