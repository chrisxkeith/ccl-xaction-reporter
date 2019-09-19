# Please credit chris.keith@gmail.com

import sys
import csv
import datetime

def log(message):
    script_name = sys.argv[0]
    print(str(datetime.datetime.now()) + '\t'+ script_name + ': ' + message)

class Reporter:
    field_indices = {}
    field_names_dict = {}

    def stripe_date(self, datetime_str):
        date_str, time_str = datetime_str.split(' ')
        mon, day, year = date_str.split('/')
        hour, minute = time_str.split(':')
        return datetime.datetime(int('20' + year), int(mon), int(day), int(hour), int(minute), 0)

    def handle_stripe(self, dict, record):
        junk, email = record['Customer Description'].split('|')
        # - compare timestamps, keep latest payment record.
        if dict.get(email.strip()):
            current_date = self.stripe_date(dict[email.strip()]['Created (UTC)'])
            new_date = self.stripe_date(record['Created (UTC)'])
            if new_date > current_date:
                dict[email.strip()] = record
        else:
            dict[email.strip()] = record

    def handle_members(self, dict, record):
        dict[record['Email']] = record

    def read_from_stream_into_dict(self, file_name, dict_processing_funct):
        dict = {}
        fieldnames = None
        with open(file_name, 'r', newline='') as infile:
            reader = csv.DictReader(infile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            fieldnames = reader.fieldnames
            for record in reader:
                dict_processing_funct(dict, record)
        log(str("{: >4d}".format(len(dict))) + ' records read from ' + file_name)
        return fieldnames, dict 

    def to_array(self, dict_record):
        arr = []
        for key, value in dict_record.items():
            arr.insert(self.field_indices[key], value)
        return arr

    def to_dict(self, array_record):
        dict_record = {}
        i = 0
        for v in array_record:
            dict_record[self.field_names_dict[i]] = v
            i += 1
        return dict_record

    def merge_payment_dates(self, stripe_dict_records, gsheets_dict_records):
        for r in gsheets_dict_records.keys():
            if stripe_dict_records.get(r):
                gsheets_dict_records.get(r)['Last Payment Date'] = stripe_dict_records.get('Created (UTC)')

    def get_record_key(self, array_record):
        lpd = ''
        if len(array_record) > self.field_indices['Last Payment Date'] and array_record[self.field_indices['Last Payment Date']]:
            lpd = array_record[self.field_indices['Last Payment Date']]
        return array_record[self.field_indices['Status']] + ' ' + lpd

    def main(self):
        stripe_fieldnames, stripe_dict_records = self.read_from_stream_into_dict(
            'STRIPE_unified_payments.csv', self.handle_stripe)
        gsheets_fieldnames, gsheets_dict_records = self.read_from_stream_into_dict(
            'Member list for export for python report - Sheet1.csv', self.handle_members)
        i = 0
        for field_name in gsheets_fieldnames:
            self.field_indices[field_name] = i
            self.field_names_dict[i] = field_name
            i += 1
        self.field_indices['Last Payment Date'] = i        
        self.field_names_dict[i] = 'Last Payment Date'        
        self.merge_payment_dates(stripe_dict_records, gsheets_dict_records)
        array_records = []
        for r in gsheets_dict_records.values():
            array_records.append(self.to_array(r))
        sorted_by_value = sorted(array_records, key=self.get_record_key)
        gsheets_fieldnames.append('Last Payment Date')
        out_file_name = 'payment_statuses.csv'
        with open(out_file_name, 'w', newline='') as outfile:
            writer = csv.DictWriter(outfile, gsheets_fieldnames, delimiter=',', quotechar='"',
                                    quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            for record in sorted_by_value:
                writer.writerow(self.to_dict(record))
        log(str("{: >4d}".format(len(sorted_by_value))) + ' records written to ' + out_file_name)

if '__main__' == __name__:
    Reporter().main()
