from unittest import TestCase
from fpdf import FPDF
import os
from .pdf_parser import raw_slides, download_slides, parse_pdf_to_single_page
from .text_extractor import extract_text
from .similarity_calculator import similarity_calc
import shutil
from luigi import LocalTarget, build


class testEducationalWeb(TestCase):
    """
    Class to test functions in educational_web.
    """
    source_folder = 'TEST-COURSE'

    @classmethod
    def setUpClass(cls):
        """
        Create 2-page toy pdf's as source input. Override the target to local.
        """
        os.mkdir(cls.source_folder)
        pdf = FPDF()
        pdf.add_page()
        pdf.set_font("Arial", size=15)
        pdf.cell(200, 10, txt="Python Test",
                 ln=1, align='C')
        pdf.add_page()
        pdf.cell(200, 10, txt="Test Case",
                 ln=1, align='C')
        pdf.output(os.path.join(cls.source_folder,"test_file.pdf"))
        raw_slides.output = lambda x: LocalTarget(cls.source_folder)

    @classmethod
    def tearDownClass(cls):
        """
        Delete generated folders.
        """
        folders_to_remove = [cls.source_folder,
                             os.path.join('raw_slides', cls.source_folder),
                             os.path.join(r'pdf.js/static/slides', cls.source_folder),
                             'slides_' + cls.source_folder,
                             os.path.join(r'static/ranking_results', cls.source_folder),
                             'idx_'+cls.source_folder]
        for folder in folders_to_remove:
            if os.path.exists(folder):
                shutil.rmtree(folder)

    def test_raw_slides(self):
        task = raw_slides()

        self.assertTrue(task.output().exists())
        self.assertTrue(task.complete())

    def test_download_slides(self):
        task = download_slides(self.source_folder)
        build([task], local_scheduler=True)
        self.assertTrue(os.path.exists(os.path.join(task.LOCAL_ROOT, self.source_folder)))
        self.assertTrue(os.path.exists(os.path.join(task.LOCAL_ROOT, self.source_folder, 'test_file.pdf')))

    def test_parse_pdf_to_single_page(self):
        task = parse_pdf_to_single_page(self.source_folder)
        build([task], local_scheduler=True)
        self.assertTrue(os.path.exists(os.path.join(task.LOCAL_ROOT, self.source_folder)))
        self.assertTrue(os.path.exists(os.path.join(task.LOCAL_ROOT, self.source_folder,'test_file',
                                                    'TEST-COURSE----test_file----slide0.pdf')))
        self.assertTrue(os.path.exists(os.path.join(task.LOCAL_ROOT, self.source_folder,'test_file',
                                                    'TEST-COURSE----test_file----slide1.pdf')))

    def test_extract_text(self):
        task = extract_text(self.source_folder)
        build([task], local_scheduler=True)
        with open(os.path.join(task.LOCAL_ROOT+'_'+self.source_folder,task.LOCAL_ROOT+'_'+self.source_folder+'.dat')) as fl:
            data = fl.readlines()
            self.assertEqual(data[0].strip(), 'python test')
            self.assertEqual(data[1].strip(), 'test case')
        with open(os.path.join(task.LOCAL_ROOT+'_'+self.source_folder,task.LOCAL_ROOT+'_'+self.source_folder+'.dat.labels')) as fl:
            data = fl.readlines()
            self.assertEqual(data[0].strip(), 'TEST-COURSE##test_file##slide0')
            self.assertEqual(data[1].strip(), 'TEST-COURSE##test_file##slide1')


    def test_similarity_calc(self):
        task = similarity_calc(self.source_folder)
        build([task], local_scheduler=True)
        self.assertTrue(os.path.exists(os.path.join(task.LOCAL_ROOT, self.source_folder)))
        self.assertTrue(os.path.exists(os.path.join(task.LOCAL_ROOT, self.source_folder,'_SUCCESS')))
