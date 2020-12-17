from csci_utils.luigi.task import Requirement, Requires, TargetOutput
from luigi import Task, Parameter
from .pdf_parser import parse_pdf_to_single_page
from csci_utils.io.new_atomic_write import atomic_write
import textract
import metapy
import re
import os
import logging
import glob

def tokenizer(str):
    """
    Given a string, cut it into list of words, and apply several rules to tokenize them.
    """
    doc = metapy.index.Document()
    doc.content(str)
    tok = metapy.analyzers.ICUTokenizer(suppress_tags=True)
    tok = metapy.analyzers.LengthFilter(tok, min=2, max=50)
    tok = metapy.analyzers.LowercaseFilter(tok)
    tok = metapy.analyzers.ListFilter(tok, "lemur-stopwords.txt", metapy.analyzers.ListFilter.Type.Reject)
    tok.set_content(doc.content())
    tok = [re.sub('[^a-zA-Z]+', '', s) for s in tok]
    return ' '.join([s for s in tok if s])+'\n'

class extract_text(Task):
    """
    Extract the text information from each page of slides
    """
    LOCAL_ROOT = r'slides'
    course_name = Parameter('CSCI-E29')
    requires = Requires()
    data = Requirement(parse_pdf_to_single_page)

    output = TargetOutput(file_pattern=LOCAL_ROOT+'_{task.course_name}', ext='')

    def run(self):
        logger = logging.getLogger('luigi-interface')
        logger.info("-----------Start extracting text from pdf's--------------")

        os.mkdir(self.output().path)
        single_pdf_file = sorted(glob.glob(os.path.join(self.data.output().path, r'**/*.pdf')))
        content = []
        label = []
        for fn in single_pdf_file:
            if fn.endswith('slide1.pdf'):
                logger.info(fn.rsplit(r'----', 1)[0])
            text = textract.process(fn)
            text = text.decode("utf-8")
            content.append(tokenizer(text))
            label.append(os.path.basename(fn).replace('----','##').replace('.pdf', '')+'\n')

        logger.info("-----------Finish extracting text from pdf's--------------")
        with atomic_write(os.path.join(self.output().path, '{}.dat'.format(self.output().path))) as fn:
            fn.writelines(content)
        with atomic_write(os.path.join(self.output().path, '{}.dat.labels'.format(self.output().path))) as fn:
            fn.writelines(label)