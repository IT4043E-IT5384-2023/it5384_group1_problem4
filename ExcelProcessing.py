import pandas as pd

def ArrayToDictionary(array):
    dic = {}
    for i in range(len(array)):
        dic[i] = array[i]
    return dic

def SaveDataToExcel(path = "add4.xlsx",data=None,append=False):
    if append != False:
        #add columns to main excel
        readdf = pd.read_excel(path, index_col=0, header=0)
        newdf = pd.concat([readdf, pd.DataFrame(data)], ignore_index=True)
        newdf.to_excel(excel_writer=path)
    else:
        newdf = pd.DataFrame(data)
        newdf.to_excel(excel_writer=path)

def GetDataFromExcel(path = "add4.xlsx"):
    readdf = pd.read_excel(path, index_col=0, header=0).T
    test=readdf.to_dict()
    arr=[]
    for key, value in test.items():
        temp=[]
        for key2,value2 in value.items():
            temp.append(value2)
        arr.append(temp)
    return arr

def ReturnDepositExcel(broken, valid_values):
    keys = []
    values = []
    arr = GetDataFromExcel()
    for key,value in arr:
        keys.append(key)
        values.append(value)
    print(keys)
    for i in range(len(broken)):
        keys.remove(broken[i])

    i=0
    a=True
    while a:
        if len(str(values[i]))<5:
            del values[i]
        else:
            i+=1
        if i==len(values):
            a = False
    print(values)
    dic = {0:{},1:{}}
    for i in range(len(keys+broken)):
        dic[0][i]=(keys+broken)[i]
        try:
            dic[1][i]=(values+valid_values)[i]
        except:
            dic[1][i] = None

    print(dic)
    SaveDataToExcel(data = dic)

SaveDataToExcel(data=[ ['0.01159', '799.00'], ['0.01160', '106.45'], ['0.01266', '249.50'], ['0.01267', '740.66'], ['0.01268', '2,115.51'], ['0.01298', '99.81'], ['0.01377', '266.81'], ['0.01503', '100.80'], ['0.03769', '150.69'], ['0.08000', '197.75'], ['0.08900', '1,095.10'], ['0.21995', '335.76'], ['0.23999', '47.07']])