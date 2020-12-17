from luigi import build
from .similarity_calculator import similarity_calc

def main():
    build([
        similarity_calc(course_name='CSCI-E29'
        )], local_scheduler=True)