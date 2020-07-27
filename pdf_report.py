from IPython.display import display, HTML
import base64
from xhtml2pdf import pisa

class PDFReport:
    def __init__(self):
        self.__image_template = '<img style="width: {width}; height: {height}" src="data:image/png;base64,{image}">'
        self.__report_html = ''

    def add_figure(self, figure, width=600, height=300):
        image = base64.b64encode(figure.to_image(format='png', width=width, height=height)).decode('utf-8')

        html_image = self.__image_template.format(image=image, width=width, height=height)
        self.__report_html += html_image

    def add_html(self, html:str):
        self.__report_html += html

    def generate_report_html(self):
        return self.__report_html

    def export_report_to_pdf(self, output_filename):
        return self.convert_html_to_pdf(self.__report_html, output_filename)

    @staticmethod
    def convert_html_to_pdf(source_html, output_filename):
        with open(output_filename, "w+b") as result_file:
            pisa_status = pisa.CreatePDF(
                    source_html,
                    dest=result_file)

        return pisa_status.err
