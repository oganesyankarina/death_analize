import pandas as pd
import numpy as np
import uuid
import logging
import statsmodels.api as sm
from datetime import date, datetime

from connect_PostGres import cnx

from ISU_death_lists_dict import df_Population, REGION, MONTHS_dict
from ISU_death_lists_dict import not_nan_filter, MKB_CODE_LIST, MKB_GROUP_LIST, MKB_GROUP_LIST_MAIN, df_MKB
from ISU_death_lists_dict import FIO_dict, escalation_recipient_list, escalation_recipient_text, df_FIO

from ISU_death_functions import make_date, make_date_born_death, make_day_week_month_year_death, calculate_death_age
from ISU_death_functions import calculate_age_group, calculate_employee_group, make_mkb, make_address
from ISU_death_functions import find_original_reason_mkb_group_name
from ISU_death_functions import time_factor_calculation, get_df_death_finished, get_db_last_index
from ISU_death_functions import make_recipient, make_corr_for_recipient, make_release_date, make_recipient_fio
from ISU_death_functions import get_db_last_index, make_escalation_recipient_fio, make_recipient_fio
from ISU_death_functions import time_factor_calculation, get_df_death_finished, get_db_last_index
from ISU_death_functions import make_recipient, make_corr_for_recipient, make_release_date, make_recipient_fio

from preprocessing import death_preprocessing
from death_rule_first_55 import death_rule_first_55
from death_rule_second import death_rule_second_new
from death_escalation import death_escalation


if __name__ == '__main__':
    logging.basicConfig(filename='logfile.log', filemode='a', format='%(asctime)s - %(levelname)s - %(message)s',
                        level=logging.INFO)
    logging.info('Start of the mortality analysis algorithm')
    start_time_ALL = datetime.now()
    print('Start of the mortality analysis algorithm')

    try:
        print('The month is over. Start forming tasks ...')
        death_preprocessing(save_to_sql=False, save_to_excel=False)
        death_rule_first_55(save_to_sql=False, save_to_excel=False)
        death_rule_second_new(save_to_sql=False, save_to_excel=False)
        death_escalation(save_to_sql=False, save_to_excel=False)
        print(f'The end of the mortality analysis algorithm. elapsed time {datetime.now() - start_time_ALL}')
        logging.info(f'The end of the mortality analysis algorithm. elapsed time {datetime.now() - start_time_ALL}')

    except Exception as e:
        logging.exception('Exception occurred')
        logging.info('The execution of the mortality analysis algorithm was not completed due to an error')
