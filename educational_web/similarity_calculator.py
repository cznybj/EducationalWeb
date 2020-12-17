import metapy
from csci_utils.luigi.task import Requirement, Requires, TargetOutput
from csci_utils.luigi.dask.target import CSVTarget
from luigi import Task, Parameter
from .text_extractor import extract_text
from csci_utils.io.new_atomic_write import atomic_write
import os
import dask.dataframe as dd
import pandas as pd
import logging
import toml


class similarity_calc(Task):
    LOCAL_ROOT = r'static/ranking_results'
    course_name = Parameter('CSCI-E29')
    requires = Requires()
    data = Requirement(extract_text)

    output = TargetOutput(file_pattern=os.path.join(LOCAL_ROOT,'{task.course_name}')+r'/',
                          ext='', target_class=CSVTarget)

    def run(self):
        logger = logging.getLogger('luigi-interface')
        logger.info("-----------Start calculating similarity--------------")
        cfg_course_path = os.path.join(self.data.output().path, 'config.toml')
        if not os.path.exists(cfg_course_path):
            cfg_course = toml.load('config.toml')
            cfg_course['dataset'] += '_' + self.course_name
            cfg_course['index'] += '_'+ self.course_name
            with atomic_write(cfg_course_path, 'w') as f:
                f.write(toml.dumps(cfg_course))
        idx = metapy.index.make_inverted_index(cfg_course_path)
        ranker = metapy.index.OkapiBM25()
        top_k = 10
        query = metapy.index.Document()
        with open(os.path.join(self.data.output().path, '{}.dat.labels'.format(self.data.output().path)),'r') as fn:
            label_list = fn.read().splitlines()
        with open(os.path.join(self.data.output().path, '{}.dat'.format(self.data.output().path)), 'r') as fn:
            txt_list = fn.read().splitlines()
        out = pd.DataFrame(columns=range(21))
        for i in range(len(label_list)):
            if not i%10:
                logger.info('processing---{}/{}'.format(str(i), len(label_list)))
            row = [label_list[i]]
            query.content(txt_list[i])
            result = ranker.score(idx, query, top_k + 1)
            if len(result) > 1:
                top_similarity = result[0]
                result = [res for res in result if res[0] != i][:top_k]
                result_normalize = [(label_list[res[0]], res[1] / top_similarity[1]) for res in result]
                result_normalize = [item for item_pair in result_normalize for item in item_pair]
                row += result_normalize
            out.loc[i, range(len(row))] = row

        out_dd = dd.from_pandas(out, npartitions=1)
        self.output().write_dask(out_dd, header=False, index=False)

        logger.info("-----------Finish calculating similarity--------------")