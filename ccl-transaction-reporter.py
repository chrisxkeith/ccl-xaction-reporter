# Please credit chris.keith@gmail.com

import sys
import csv
import datetime
import os.path
from os import path

def log(message):
    script_name = sys.argv[0]
    print(str(datetime.datetime.now()) + '\t'+ script_name + ': ' + message)

class Reporter:
    field_indices = {}
    field_names_dict = {}
    gsheets_fieldnames = []
    gsheets_dict_records = {}
    paypal_to_membership_email_mapping = {'rolfvw@pizzicato.com' : 'rolfvw@gmail.com', 'alan@halo.nu' : 'alanrockefeller@gmail.com'}
    # Cancelled membership: 'matthew.stewart.mi@gmail.com' : 'codehesionoakland@gmail.com'

    def stripe_date(self, datetime_str):
        date_str, time_str = datetime_str.split(' ')
        year, month, day = date_str.split('-')
        dt = datetime.datetime(int(year), int(month), int(day), 0, 0, 0)
        return dt.strftime('%Y/%m/%d')

    def find_latest_record(self, dict, record, email, date_col_name):
        # - compare timestamps, keep latest payment record.
        if dict.get(email.strip()):
            current_date = dict[email.strip()][date_col_name]
            new_date = record[date_col_name]
            if new_date > current_date:
                dict[email.strip()] = record
        else:
            dict[email.strip()] = record

    def handle_stripe(self, dict, record):
        if '|' in record['Customer Description']:
            junk, email = record['Customer Description'].split('|')
            record['Created (UTC)'] = self.stripe_date(record['Created (UTC)'])
            self.find_latest_record(dict, record, email, 'Created (UTC)')

    def handle_paypal(self, dict, record):
        if 'Credit' == record['Balance Impact']:
            if self.paypal_to_membership_email_mapping.get(record['From Email Address']):
                record['From Email Address'] = self.paypal_to_membership_email_mapping.get(record['From Email Address'])
            # Test Type and Note columns to detect if this is a membership dues payment.
            if 'ubscription' in record['Type'] or 'ember' in record['Note'] or 'ues' in record['Note'] \
                    or self.gsheets_dict_records.get(record['From Email Address']):
                mon, day, year = record['Date'].split('/')
                dt = datetime.datetime(int(year), int(mon), int(day), 0, 0, 0)
                record['Date'] =  dt.strftime('%Y/%m/%d')
                self.find_latest_record(dict, record, record['From Email Address'], 'Date')

    def get_delinquent_column_header(self):
        return 'Months Delinquent'

    def handle_members(self, dict, record):
        for n in ['Expected Payment Amount',
                'Last Payment Date',
                'Last Payment Amount',
                self.get_delinquent_column_header(),
                'Payment Method',
            ]:
            record[n] = ''
        record['Email'] = record['Email'].strip()
        dict[record['Email']] = record

    def handle_new_sheet(self, dict, record):
        dict[record['Email']] = record

    def read_from_stream_into_dict(self, file_name, dict_processing_funct):
        dict = {}
        fieldnames = None
        with open(file_name, 'r', newline='') as infile:
            reader = csv.DictReader(infile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            fieldnames = reader.fieldnames
            c = 1
            try:
                # Kludge around this error (Mac file ending char?):
                # UnicodeDecodeError: 'charmap' codec can't decode byte 0x81 in position 2340: character maps to <undefined>
                for record in reader:
                    dict_processing_funct(dict, record)
                    c += 1
            except:
                print('Failed on record ' + str(c))
        log(str("{: >4d}".format(len(dict))) + ' records read from ' + file_name)
        return fieldnames, dict 

    def find_latest_payment(self, gsheets_rec, date_str):
        gsheets_rec['Last Payment Date'] = date_str
        last_paid_date = datetime.datetime.strptime(date_str, '%Y/%m/%d')
        tdiff = datetime.datetime.now() - last_paid_date
        if (tdiff.days > 30):
            gsheets_rec[self.get_delinquent_column_header()] = str(int(tdiff.days / 30)) # approximate months, not exact.

    def update_statuses(self):
        status_overrides = {
            '???' : 'Cancelled',
            'alexandra.c.hay@gmail.com' : 'Pending',
            'andrew.mathau@gmail.com' : 'Cancelled',
            'aprilsteed@gmail.com' : 'Pending',
            'arin.pavlov@gmail.com' : 'Cancelled',
            'cherylching26@gmail.com' : 'Cancelled',
            'chris.keith@gmail.com' : 'Pending',
            'christophernoel84@gmail.com' : 'Cancelled',
            'dansantos88@gmail.com' : 'Pending',
            'ddigor@well.com' : 'Cancelled',
            'ferrinmax@gmail.com' : 'Pending',
            'graceharrisj@gmail.com' : 'Pending',
            'harte.singer@gmail.com' : 'Pending',
            'jessicakarma@gmail.com' : 'Cancelled',
            'jonathan.ling.pan@???' : 'Cancelled',
            'jonathanbutler@protomail.com' : 'Cancelled',
            'josh.mcmenemy@gmail.com' : 'Cancelled',
            'jwelcher@gmail.com' : 'Cancelled',
            'linalee128@gmail.com' : 'Pending',
            'mattpallota5@gmail.com' : 'Pending',
            'mgabiati@gmail.com' : 'Cancelled',
            'pat.coffey@gmail.com' : 'Cancelled',
            'pellaea@gmail.com' : 'Cancelled',
            'petersongarrettjames@gmail.com' : 'Pending',
            'pupusworm@gmail.com' : 'Cancelled',
            'receipts@ianmatthews.com' : 'Pending',
            'spiritoftwotimes@gmail.com' : 'Cancelled',
            'stharlow@gmail.com' : 'Cancelled',
            'supernovatova@gmail.com' : 'Pending',
            'thornton.thompson@gmail.com' : 'Cancelled',
            'tiarenoelle@gmail.com' : 'Cancelled',
            'tinaekhtiar@gmail.com' : 'Cancelled',
        }
        for k, v in status_overrides.items():
            if self.gsheets_dict_records.get(k):
                self.gsheets_dict_records.get(k)['Status'] = v

    def update_payment_amounts(self):
        payment_amounts = {
            'ajiboyeifedayo@gmail.com' : '80',
            'alanrockefeller@gmail.com' : '25',
            'aleclourenco@gmail.com' : '80',
            'alexandra.c.hay@gmail.com' : '',
            'andreas.j.albrecht@gmail.com' : '80',
            'arent1506@gmail.com' : '20',
            'arianaccisneros@gmail.com' : '20',
            'arin.pavlov@gmail.com' : '20',
            'bobbie2882@gmail.com' : '0',
            'cassady3@tdl.com' : '30',
            'ceremona@gmail.com' : '0',
            'cherylching26@gmail.com' : '10',
            'chrisken@gmail.com' : '20',
            'christophernoel84@gmail.com' : '20',
            'daniel@everflux.tech' : '80',
            'david@tinialloy.com' : '80',
            'dcandrsn@aol.com' : '80',
            'ddigor@well.com' : '80',
            'debbiej.klein@gmail.com' : '80',
            'di.franco@gmail.com' : '80',
            'dougchang25@gmail.com' : '80',
            'gustometry@gmail.com' : '80',
            'iva.brzon@gmail.com' : '80',
            'ivelinavramov@gmail.com' : '0',
            'jacob.statnekov@gmail.com' : '80',
            'jakekeithkeller@gmail.com' : '10',
            'jcs.ces@gmail.com' : '20',
            'jjeasterday6@gmail.com' : '20',
            'jlampe18@gmail.com' : '20',
            'jnr424@gmail.com' : '0',
            'joelmartinez@springmail.com' : '0',
            'josh.mcmenemy@gmail.com' : '80',
            'josiah.zayner@gmail.com' : '80',
            'jwelcher@gmail.com' : '80',
            'kingmushrooms@gmail.com' : '20',
            'lbmenchaca@berkeley.edu' : '40',
            'litchfield.ken@gmail.com' : '20',
            'Matt.lims@gmail.com' : '0', # TBD : Should this be all lowercase?
            'mgabiati@gmail.com' : '80',
            'muldavin.m@gmail.com' : '50',
            'natarajn@aol.com' : '80',
            'nenufarmoleculesforlife@gmail.com' : '40',
            'noelcarrascal@gmail.com' : '0',
            'nsipplswezey@gmail.com' : '20',
            'patrikd@gmail.com' : '0',
            'paul.millet@hotmail.com' : '80',
            'pellaea@gmail.com' : '80',
            'ramy.kim@gmail.com' : '80',
            'richard.h.ho@gmail.com' : '20',
            'rikke.c.rasmussen@gmail.com' : '80',
            'rolfvw@gmail.com' : '80',
            'spiritoftwotimes@gmail.com' : '20',
            'stharlow@gmail.com' : '80',
            'sudohumans@juul.io' : '80',
            'thalula@peralta.edu' : '80',
            'thornton.thompson@gmail.com' : '20',
            'tiarenoelle@gmail.com' : '20',
            'tim.dobbs@gmail.com' : '80',
            'tinaekhtiar@gmail.com' : '20',
            'tomahawk.jara@gmail.com' : '0',
        }
        for k, v in payment_amounts.items():
            if self.gsheets_dict_records.get(k):
                self.gsheets_dict_records.get(k)['Expected Payment Amount'] = str(v)
    
    def merge_payment_dates(self, stripe_dict_records, paypal_dict_records):
        for r in self.gsheets_dict_records.keys():
            gsheets_rec = self.gsheets_dict_records.get(r)
            gsheets_rec['Payment Method'] = 'n/a'
            if gsheets_rec['Status'] == 'Current' and gsheets_rec.get('Expected Payment Amount') and int(gsheets_rec['Expected Payment Amount']) > 0:
                if gsheets_rec['Email'] in ['litchfield.ken@gmail.com', 'thalula@peralta.edu', 'natarajn@aol.com', 'nenufarmoleculesforlife@gmail.com']:
                    gsheets_rec['Payment Method'] = 'cash'
                else:
                    if stripe_dict_records.get(r):
                        stripe_rec = stripe_dict_records.get(r)
                        gsheets_rec['Payment Method'] = 'Stripe'
                        date_str = stripe_rec.get('Created (UTC)')
                        self.find_latest_payment(gsheets_rec, date_str)
                        gsheets_rec['Last Payment Amount'] = stripe_rec.get('Amount')
                    else:
                        if paypal_dict_records.get(r):
                            paypal_rec = paypal_dict_records.get(r)
                            gsheets_rec['Payment Method'] = 'PayPal'
                            date_str = paypal_rec.get('Date')
                            self.find_latest_payment(gsheets_rec, date_str)
                            gsheets_rec['Last Payment Amount'] = paypal_rec.get('Gross')
                        else:
                            gsheets_rec['Payment Method'] = 'unknown'

    def write_payment_statuses(self):
        c = 0
        for r in self.gsheets_dict_records.values():
            if r.get('Status') == 'Current':
                c += 1
        self.gsheets_fieldnames.append('Current members: ' + str(c))
        out_file_name = 'payment_statuses.csv'
        with open(out_file_name, 'w', newline='') as outfile:
            writer = csv.DictWriter(outfile, self.gsheets_fieldnames, delimiter=',', quotechar='"',
                                    quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            for key in sorted(self.gsheets_dict_records.keys()):
                writer.writerow(self.gsheets_dict_records[key])
        log(str("{: >4d}".format(len(self.gsheets_dict_records))) + ' records written to ' + out_file_name)

    def write_full_email_list(self, stripe_dict_records):
        master_list = {}
        for k in stripe_dict_records.keys():
            master_list[k] = {'Email' : k, 'In Stripe' : 'Y', 'In Google Sheet' : '',
                'Stripe payment date' : stripe_dict_records[k]['Created (UTC)'],
                'Notes' : ''}
        for k in self.gsheets_dict_records.keys():
            if master_list.get(k):
                master_list.get(k)['In Google Sheet'] = 'Y'
                master_list.get(k)['Notes'] = self.gsheets_dict_records[k]['Notes']
            else:
                master_list[k] = {'Email' : k, 'In Stripe' : '', 'In Google Sheet' : 'Y',
                    'Stripe payment date' : '', 'Notes' : self.gsheets_dict_records[k]['Notes'] }
        field_names = ['Email', 'In Stripe', 'In Google Sheet', 'Stripe payment date', 'Notes']
        field_indices = {'Email' : 1, "In Stripe": 2, 'In Google Sheet' : 3,
            'Stripe payment date' : 4, 'Notes' : 5}
        self.write_dict_to_csv('full_email_list.csv', field_names, master_list)

    def write_dict_to_csv(self, out_file_name, field_names, dict):
        with open(out_file_name, 'w', newline='') as outfile:
            writer = csv.DictWriter(outfile, field_names, delimiter=',', quotechar='"',
                                    quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            for record in dict.items():
                writer.writerow(record[1])
        log(str("{: >4d}".format(len(dict))) + ' records written to ' + out_file_name)

    def write_unknown_stripe_emails(self, stripe_dict_records):
        unknown_emails = {}
        for k in stripe_dict_records.keys():
            if not self.gsheets_dict_records.get(k):
                unknown_emails[k] = {'Email' : k, 'Stripe payment date' : stripe_dict_records[k]['Created (UTC)'] }
        field_names = ['Email', 'Stripe payment date']
        field_indices = {'Email' : 1, 'Stripe payment date' : 2}
        self.write_dict_to_csv('unknown_stripe_emails.csv', field_names, unknown_emails)

    def setup_columns(self):
        self.gsheets_fieldnames = [
            'First Name',
            'Family (Last) Name',
            'Email',
            'Status',
            'Expected Payment Amount',
            'Last Payment Amount',
            'Last Payment Date',
            self.get_delinquent_column_header(),
            'Payment Method',
            'Notes',
            'Membership Agreement Date',
            'Address',
            'Phone',
        ]
        i = 0
        for field_name in self.gsheets_fieldnames:
            self.field_indices[field_name] = i
            self.field_names_dict[i] = field_name
            i += 1

    def merge_old_data(self):
        old_gsheets_fieldnames, old_gsheets_dict_records = self.read_from_stream_into_dict(
                'Member list for export for python report - Sheet1.csv',
                self.handle_members)
        for r in self.gsheets_dict_records.keys():
            gsheets_rec = self.gsheets_dict_records.get(r)
            old_rec = old_gsheets_dict_records[r]
            for field_name in ['First Name',
                'Family (Last) Name',
                'Notes',
                'Membership Agreement Date',
                'Address',
                'Phone',
            ]:
                if old_rec.get(field_name):
                    gsheets_rec[field_name] = old_rec[field_name]
    
    def main(self):
        if path.exists('payment statuses - payment_statuses.csv'):
            self.gsheets_fieldnames, self.gsheets_dict_records = self.read_from_stream_into_dict(
                'payment statuses - payment_statuses.csv',
                self.handle_new_sheet)
            self.merge_old_data()
        else:
            self.gsheets_fieldnames, self.gsheets_dict_records = self.read_from_stream_into_dict(
                'Member list for export for python report - Sheet1.csv',
                self.handle_members)
        self.setup_columns()
        self.update_statuses()
        self.update_payment_amounts()
        stripe_fieldnames, stripe_dict_records = self.read_from_stream_into_dict(
            'STRIPE_payments2019.csv',
            self.handle_stripe)
        paypal_fieldnames, paypal_dict_records = self.read_from_stream_into_dict(
            'PayPal_Payments2019.csv',
            self.handle_paypal)
        self.merge_payment_dates(stripe_dict_records, paypal_dict_records)
        self.write_payment_statuses()
        self.write_full_email_list(stripe_dict_records)
        self.write_unknown_stripe_emails(stripe_dict_records)

if '__main__' == __name__:
    Reporter().main()
