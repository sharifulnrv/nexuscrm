def export_to_csv(df, filename='export.csv'):
    df.to_csv(filename, index=False)
    return filename

def export_to_excel(df, filename='export.xlsx'):
    df.to_excel(filename, index=False, engine='openpyxl')
    return filename
