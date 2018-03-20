#!/usr/bin/env python
# Author: Duncan Tormey
# Email: dut@stowers.org or duncantormey@gmail.com
import pandas as pd
from glob import glob
from pprint import pprint


class LabData(object):
    def __init__(self, lab_path, lab_strings):
        self.lab_path = lab_path
        self.lab_strings = lab_strings
        self.lab_name = [d for d in lab_path.split('/') if d][-1]
        self.parse_order_data()

    def __repr__(self):
        return self.lab_data.to_string()

    def __str__(self):
        return self.lab_data.to_string()

    def gen_order_data(self):
        molng_wild_card = '%s/*/*' % self.lab_path
        order_paths = glob(molng_wild_card)
        for path in order_paths:
            orderer = path.split('/')[-2]
            order_id = path.split('/')[-1]
            yield path, orderer, order_id

    def gen_sample_report_paths(self, path):
        pass

    def parse_order_data(self):
        for path, orderer, order_id in self.gen_order_data():
            #
            if 'MOLNG' in order_id:
                if any('Sample_Report.csv' in p for p in glob('{}/*/*'.format(path))):
                    pass
                else:
                    print( '*'*40 + 'WARNING {}'.format(order_id) + '*'*40 )
                    print(path, orderer, order_id)
                    pprint(glob('{}/*'.format(path)))

    def ret_sample_report_df(self, path):
        records = []
        with open(path, 'r') as f:
            for i, line in enumerate(f):
                if i == 0:
                    header = line.split(',')
                elif any(lab_string in line for lab_string in self.lab_strings):
                    record = line.split(',')
                    records.append(record)
                else:
                    print('Excluding Line {}: {}'.format(line))
        try:
            sample_df = pd.DataFrame.from_records(records, columns=header)
        except:
            pprint(records)
        sample_df['report_path'] = path
        sample_df['flowcell'] = path.split('/')[-2]

        return sample_df


def main():
    lab_path = '/n/analysis/Baumann'
    lab_strings = ['Baumann', 'Baumann Lab', 'The Baumann Lab']
    baumann_lab = LabData(lab_path=lab_path, lab_strings=lab_strings)
    # pprint(baumann_lab.lims_data.columns)
    # pprint(baumann_lab.lims_data.lab.unique())


if __name__ == "__main__":
    main()
