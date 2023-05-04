def read_imdb(session, f_in, schema_df):
    '''

    :param session: spark_session
    :param f_in: file with data
    :param schema_df: schema dataset
    :return: dataFrame
    '''
    df_in = session.read.csv(f_in, header = True, nullValue = 'null', schema = schema_df, sep='\t')
    return df_in

def write_result(f_out, df):
    '''

    :param f_out: file for result
    :param df: dataFrame
    :return:
    '''
    df.write.csv(f_out, header = True, mode = 'overwrite')