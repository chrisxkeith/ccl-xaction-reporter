# Please credit chris.keith@gmail.com

import sys
import csv
import datetime

def log(message):
    script_name = sys.argv[0]
    print(str(datetime.datetime.now()) + '\t'+ script_name + ': ' + message)

class Reporter:
    def stripe_date(self, datetime_str):
        date_str, time_str = datetime_str.split(' ')
        mon, day, year = date_str.split('/')
        hour, minute = time_str.split(':')
        return datetime.datetime(int('20' + year), int(mon), int(day), int(hour), int(minute), 0)

    def handle_stripe(self, dict, key_column_name, record):
        junk, email = record['Customer Description'].split('|')
        # - compare timestamps, keep latest payment record.
        if dict.get(email.strip()):
            current_date = self.stripe_date(dict[email.strip()]['Created (UTC)'])
            new_date = self.stripe_date(record['Created (UTC)'])
            if new_date > current_date:
                dict[email.strip()] = record
        else:
            dict[email.strip()] = record

    def handle_members(self, dict, key_column_name, record):
        dict[record[key_column_name]] = record

    def read_from_stream_into_dict(self, file_name, key_column_name, dict_processing_funct):
        dict = {}
        fieldnames = None
        with open(file_name, 'r', newline='') as infile:
            reader = csv.DictReader(infile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            fieldnames = reader.fieldnames
            for record in reader:
                dict_processing_funct(dict, key_column_name, record)
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

    def main(self):
        stripe_fieldnames, stripe_dict_records = self.read_from_stream_into_dict(
            'STRIPE_unified_payments.csv', 'Customer Email', self.handle_stripe)
        gsheets_fieldnames, gsheets_dict_records = self.read_from_stream_into_dict(
            'CCL Members Master List - Current Member List.csv', 'Email', self.handle_members)
        array_records = []
        for r in stripe_dict_records.values():
            array_records.append(self.to_array(r))
        sorted_by_value = sorted(array_records, key=self.get_record_key)
        out_file_name = 'payment_statuses.csv'
        with open(out_file_name, 'w', newline='') as outfile:
            writer = csv.DictWriter(outfile, stripe_fieldnames, delimiter=',', quotechar='"',
                                    quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            for record in sorted_by_value:
                writer.writerow(self.to_dict(record))
        log(str("{: >4d}".format(len(sorted_by_value))) + ' records written to' + out_file_name)

if '__main__' == __name__:
    Reporter().main()
