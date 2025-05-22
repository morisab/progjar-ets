import logging
import os
import json
import base64
from glob import glob


class FileInterface:
    def __init__(self):
            try:
                # Dapatkan path absolut ke direktori saat ini
                base_dir = os.path.dirname(os.path.abspath(__file__))
                
                # Normalisasi path untuk cross-platform compatibility
                self.files_dir = os.path.normpath(os.path.join(base_dir, 'files'))
                
                # Buat direktori dengan permission yang tepat
                os.makedirs(self.files_dir, mode=0o755, exist_ok=True)
                
                # Verifikasi direktori bisa diakses
                if not os.path.isdir(self.files_dir):
                    raise RuntimeError(f"Failed to create or access directory: {self.files_dir}")
                
                self.original_dir = os.getcwd()
                logging.info(f"File storage initialized at: {self.files_dir}")
                
            except Exception as e:
                logging.error(f"Directory initialization failed: {str(e)}")
                raise

    def _change_to_files_dir(self):
        try:
            os.chdir(self.files_dir)
        except Exception as e:
            logging.error(f"Failed to change to files directory: {str(e)}")
            raise

    def _restore_original_dir(self):
        try:
            os.chdir(self.original_dir)
        except Exception as e:
            logging.error(f"Failed to restore original directory: {str(e)}")
            raise


    def upload(self, params=[]):
        try:
            self._change_to_files_dir()
            filename = params[0]
            filedata = params[1]
            decoded_data = base64.b64decode(filedata.encode())
            with open(filename, 'wb') as f:
                f.write(decoded_data)
            return dict(status='OK', data=f"{filename} uploaded successfully")
        except Exception as e:
            return dict(status='ERROR', data=str(e))
        finally:
            self._restore_original_dir()


    def delete(self, params=[]):
        try:
            self._change_to_files_dir()
            filename = params[0]
            os.remove(filename)
            return dict(status='OK', data=f"{filename} deleted successfully")
        except Exception as e:
            return dict(status='ERROR', data=str(e))
        finally:
            self._restore_original_dir()

    def list(self,params=[]):
        try:
            self._change_to_files_dir()
            filelist = glob('*.*')
            return dict(status='OK',data=filelist)
        except Exception as e:
            return dict(status='ERROR',data=str(e))
        finally:
            self._restore_original_dir()

    def get(self,params=[]):
        try:
            self._change_to_files_dir()
            filename = params[0]
            if (filename == ''):
                return None
            fp = open(f"{filename}",'rb')
            isifile = base64.b64encode(fp.read()).decode()
            return dict(status='OK',data_namafile=filename,data_file=isifile)
        except Exception as e:
            return dict(status='ERROR',data=str(e))
        finally:
            self._restore_original_dir()


if __name__=='__main__':
    f = FileInterface()
    print(f.list())
    print(f.get(['pokijan.jpg']))
