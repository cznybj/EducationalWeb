from csci_utils.luigi.task import Requirement, Requires, TargetOutput
from luigi import Task, ExternalTask, Parameter
from luigi.contrib.s3 import S3Target
import glob
import logging
import os
from PyPDF2 import PdfFileWriter, PdfFileReader
from shutil import copytree


class raw_slides(ExternalTask):
    SLIDES_ROOT = r's3://cznybj/raw_slides'
    course_name = Parameter('CSCI-E29')

    output = TargetOutput(file_pattern=SLIDES_ROOT+r'/{task.course_name}', ext='', target_class=S3Target)

class download_slides(Task):
    LOCAL_ROOT = 'raw_slides'
    course_name = Parameter('CSCI-E29')

    requires = Requires()
    data = Requirement(raw_slides)

    output = TargetOutput(file_pattern=os.path.join(LOCAL_ROOT,'{task.course_name}'), ext='')

    def run(self):
        logger = logging.getLogger('luigi-interface')
        logger.info("-----------Start downloading pdf's--------------")

        if isinstance(self.data.output(), S3Target):
            os.mkdir(self.output().path)
            for file in self.data.output().fs.list(self.data.output().path):
                if file:
                    self.data.output().fs.get(s3_path=self.data.output().path+r'/'+file,
                                              destination_local_path=os.path.join(self.output().path, file))
        else:
            copytree(self.data.output().path, self.output().path)
        logger.info("-----------Finish downloading pdf's--------------")

class parse_pdf_to_single_page(Task):
    LOCAL_ROOT = r'pdf.js/static/slides'
    course_name = Parameter('CSCI-E29')
    requires = Requires()
    data = Requirement(download_slides)

    output = TargetOutput(file_pattern=os.path.join(LOCAL_ROOT,'{task.course_name}'), ext='')

    def run(self):
        logger = logging.getLogger('luigi-interface')
        logger.info("-----------Start parsing pdf's--------------")
        os.mkdir(self.output().path)
        raw_pdf_file = sorted(glob.glob(os.path.join(self.data.output().path, '*.pdf')))

        for fn in raw_pdf_file:
            logger.info(fn)
            inputpdf = PdfFileReader(open(fn, "rb"))
            folder_name = os.path.join(self.output().path, os.path.basename(fn).replace('.pdf', ''))
            os.mkdir(folder_name)
            for i in range(inputpdf.numPages):
                output = PdfFileWriter()
                output.addPage(inputpdf.getPage(i))
                file_name = os.path.join(folder_name, "{course_name}----{folder}----slide{slide_num}.pdf".format(
                    course_name=self.course_name, folder=os.path.basename(fn).replace('.pdf', ''), slide_num=str(i)))

                with open(file_name, "wb") as outputStream:
                    output.write(outputStream)

        logger.info("-----------Finish parsing pdf's--------------")
