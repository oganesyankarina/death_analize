import logging
from datetime import datetime

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
        death_preprocessing(save_to_sql=True, save_to_excel=False)
        death_rule_first_55(save_to_sql=True, save_to_excel=True)
        death_rule_second_new(save_to_sql=True, save_to_excel=True)
        death_escalation(save_to_sql=True, save_to_excel=True)
        print(f'The end of the mortality analysis algorithm. elapsed time {datetime.now() - start_time_ALL}')
        logging.info(f'The end of the mortality analysis algorithm. elapsed time {datetime.now() - start_time_ALL}')

    except Exception as e:
        logging.exception('Exception occurred')
        logging.info('The execution of the mortality analysis algorithm was not completed due to an error')
