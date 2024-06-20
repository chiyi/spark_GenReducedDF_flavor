import sys, datetime


def usage():
    print('usage: select_date_HHMM(20240619_1610) output_filename')
    print('')
    sys.exit(-1)


def chk_para(inp_para):
    if len(inp_para) < 3:
        usage()
    par1 = inp_para[1]
    try:
        dt_obj = datetime.datetime.strptime(par1, "%Y%m%d_%H%M")
    except:
        usage()
    if dt_obj.year < 2000:
        usage()
    if len(inp_para[2]) < 3:
        usage()
