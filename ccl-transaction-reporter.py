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
    gsheets_fieldnames = []
    gsheets_dict_records = {}
    paypal_to_membership_email_mapping = {'rolfvw@pizzicato.com' : 'rolfvw@gmail.com', 'alan@halo.nu' : 'alanrockefeller@gmail.com'}
    # Cancelled membership: 'matthew.stewart.mi@gmail.com' : 'codehesionoakland@gmail.com'

    def to_std_date_fmt(self, date_str):
        mon, day, year = date_str.split('/')
        dt = datetime.datetime(int('20' + year), int(mon), int(day), 0, 0, 0)
        return dt.strftime('%Y/%m/%d')

    def stripe_date(self, datetime_str):
        date_str, time_str = datetime_str.split(' ')
        return self.to_std_date_fmt(date_str)

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
                record['Date'] = self.to_std_date_fmt(record['Date'])
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

    def find_latest_payment(self, gsheets_rec, date_str):
        gsheets_rec['Last Payment Date'] = date_str
        last_paid_date = datetime.datetime.strptime(date_str, '%Y/%m/%d')
        tdiff = datetime.datetime.now() - last_paid_date
        if (tdiff.days > 30):
            gsheets_rec[self.get_delinquent_column_header()] = str(int(tdiff.days / 30)) # approximate months, not exact.

    def update_statuses(self):
        status_overrides = {
            'jonathanbutler@protomail.com' : 'Cancelled',
            'jessicakarma@gmail.com' : 'Cancelled',
            'josh.mcmenemy@gmail.com' : 'Pending',
            'arin.pavlov@gmail.com' : 'Cancelled',
            'tiarenoelle@gmail.com' : 'Cancelled',
            '???' : 'Cancelled',
            'pupusworm@gmail.com' : 'Cancelled',
            'pat.coffey@gmail.com' : 'Cancelled',
            'alexandra.c.hay@gmail.com' : 'Pending',
            'ferrinmax@gmail.com' : 'Pending',
            'graceharrisj@gmail.com' : 'Pending',
            'chris.keith@gmail.com' : 'Pending',
            'linalee128@gmail.com' : 'Pending',
            'andrew.mathau@gmail.com' : 'Pending',
            'receipts@ianmatthews.com' : 'Pending',
            'mattpallota5@gmail.com' : 'Pending',
            'jonathan.ling.pan@???' : 'Pending',
            'petersongarrettjames@gmail.com' : 'Pending',
            'dansantos88@gmail.com' : 'Pending',
            'harte.singer@gmail.com' : 'Pending',
            'supernovatova@gmail.com' : 'Pending',
            'aprilsteed@gmail.com' : 'Pending',
        }
        for k, v in status_overrides.items():
            if self.gsheets_dict_records.get(k):
                self.gsheets_dict_records.get(k)['Status'] = v

    def update_payment_amounts(self):
        payment_amounts = {
            'mgabiati@gmail.com' : '80',
            'alexandra.c.hay@gmail.com' : '80',
            'debbiej.klein@gmail.com' : '80',
            'josh.mcmenemy@gmail.com' : '80',
            'paul.millet@hotmail.com' : '80',
            'aleclourenco@gmail.com' : '80',
            'di.franco@gmail.com' : '80',
            'sudohumans@juul.io' : '80',
            'gustometry@gmail.com' : '80',
            'daniel@everflux.tech' : '80',
            'josiah.zayner@gmail.com' : '80',
            'tim.dobbs@gmail.com' : '80',
            'rikke.c.rasmussen@gmail.com' : '80',
            'andreas.j.albrecht@gmail.com' : '80',
            'ajiboyeifedayo@gmail.com' : '80',
            'ramy.kim@gmail.com' : '80',
            'david@tinialloy.com' : '80',
            'iva.brzon@gmail.com' : '80',
            'ddigor@well.com' : '80',
            'dcandrsn@aol.com' : '80',
            'jacob.statnekov@gmail.com' : '80',
            'stharlow@gmail.com' : '80',
            'pellaea@gmail.com' : '80',
            'dougchang25@gmail.com' : '80',
            'jwelcher@gmail.com' : '80',
            'rolfvw@gmail.com' : '80',
            'thalula@peralta.edu' : '80',
            'natarajn@aol.com' : '80',
            'muldavin.m@gmail.com' : '50',
            'lbmenchaca@berkeley.edu' : '40',
            'nenufarmoleculesforlife@gmail.com' : '40',
            'cassady3@tdl.com' : '30',
            'alanrockefeller@gmail.com' : '25',
            'arianaccisneros@gmail.com' : '20',
            'kingmushrooms@gmail.com' : '20',
            'arin.pavlov@gmail.com' : '20',
            'tiarenoelle@gmail.com' : '20',
            'nsipplswezey@gmail.com' : '20',
            'jcs.ces@gmail.com' : '20',
            'tinaekhtiar@gmail.com' : '20',
            'spiritoftwotimes@gmail.com' : '20',
            'chrisken@gmail.com' : '20',
            'jjeasterday6@gmail.com' : '20',
            'thornton.thompson@gmail.com' : '20',
            'arent1506@gmail.com' : '20',
            'jlampe18@gmail.com' : '20',
            'richard.h.ho@gmail.com' : '20',
            'christophernoel84@gmail.com' : '20',
            'litchfield.ken@gmail.com' : '20',
            'jakekeithkeller@gmail.com' : '10',
            'cherylching26@gmail.com' : '10',
            'ivelinavramov@gmail.com' : '0',
            'noelcarrascal@gmail.com' : '0',
            'ceremona@gmail.com' : '0',
            'patrikd@gmail.com' : '0',
            'Matt.lims@gmail.com' : '0', # TBD : Should this be all lowercase?
            'tomahawk.jara@gmail.com' : '0',
            'bobbie2882@gmail.com' : '0',
            'joelmartinez@springmail.com' : '0',
            'jnr424@gmail.com' : '0',
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
            for record in self.gsheets_dict_records.values():
                writer.writerow(record)
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

    def main(self):
        self.gsheets_fieldnames, self.gsheets_dict_records = self.read_from_stream_into_dict(
            'Member list for export for python report - Sheet1.csv',
            self.handle_members)
        self.setup_columns()
        self.update_statuses()
        self.update_payment_amounts()
        stripe_fieldnames, stripe_dict_records = self.read_from_stream_into_dict(
            'STRIPE_unified_payments.csv',
            self.handle_stripe)
        paypal_fieldnames, paypal_dict_records = self.read_from_stream_into_dict(
            'PayPalData2019.csv',
            self.handle_paypal)
        self.merge_payment_dates(stripe_dict_records, paypal_dict_records)
        self.write_payment_statuses()
        self.write_full_email_list(stripe_dict_records)
        self.write_unknown_stripe_emails(stripe_dict_records)

if '__main__' == __name__:
    Reporter().main()
