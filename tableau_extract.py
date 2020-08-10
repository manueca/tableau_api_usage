import tableauserverclient as TSC
import pandas as pd
from io import StringIO

class Tableau_Server(object):
    
    """docstring for ClassName"""
    def __init__(self,username, password,site_id,url, https = False):
        super().__init__() 
        self.all_books_names=[]
        self.all_books=[]
        # authorize 
        tableau_auth = TSC.TableauAuth(username, password, site_id)
        server = TSC.Server(url)
        server.add_http_options({'verify': https}) # if not https server skip warnings
        server.auth.sign_in(tableau_auth)
        server.use_highest_version() # make sure to use the same api version as server
        server.server_info.get()
        
        self.server = server
        
        # get all workbooks in site
        self.all_books, pagination_item = self.server.workbooks.get()
        #self.all_books_names = [wb.name for wb in self.all_books]
        for wb in TSC.Pager(server.workbooks):
             self.all_books_names.append(wb.name)
             self.all_books.append(wb)
        print('\nThere are {} workbooks on "{}" Tableau Server site.'.format(pagination_item.total_available,site_id))

    def check_wb_name(self,wb_name):
        # check if workbook exists
        print (self.all_books_names)
        if wb_name not in self.all_books_names:
            print('Workbook with name {} not found.'.format(wb_name))
            return


    def download_wb(self,wb_name):
        # downlaod workbook given workbook name
        self.check_wb_name(wb_name)
        for w in self.all_books:
            print(w.name)
        wb = [w for w in self.all_books if w.name == wb_name][0]
        self.server.workbooks.download(wb.id)
    
    
    def download_view_csv(self,wb_name, view_name = None):
        # downlaod view csv given workbook name and view name (optional)
        self.check_wb_name(wb_name)
        wb = [w for w in self.all_books if w.name == wb_name][0]
        # request views
        self.server.workbooks.populate_views(wb) 
        views = [view for view in wb.views]
        view_names = [view.name for view in wb.views]
        
        if view_name is not None and view_name not in view_names:
            print('View with name "{}" not found in workbook "{}". Below are available views.'.format(view_name,wb_name))
            print(view_names)
            return
        
        # get either first view or user defined view
        view_item = [view for view in views][0]
        if view_name is not None:
            view_item = [view for view in views if view.name == view_name][0]
        self.server.views.populate_csv(view_item) # request view csv
        
        # Perform byte join on the CSV data
        string = StringIO(b''.join(view_item.csv).decode("utf-8"))
        df = pd.read_csv(string, sep=",")
        # pivot view csv so its in wide format if there is "Measure Values" column
        # if not just save to csv
        if 'Measure Values' not in df.columns.values:
            df.to_csv('{}.csv'.format(view_item.name), index = False)
            return
        df['Measure Values'] = pd.to_numeric(df['Measure Values'].str.replace('\\,|\\$|\\%', ''))
        cols = [c for c in df.columns.values if c not in ('Measure Values','Measure Names')]
        df = pd.pivot_table(df,values = 'Measure Values', columns = 'Measure Names', index = cols).reset_index()
        df.to_csv('{}.csv'.format(view_item.name), index = False)

def main():
    username = ''
    password = ''
    site_id = ''
    url = ''

    ts = Tableau_Server(username,password,site_id,url)
    
    # download a workbook
    #ts.download_wb('')
    
    # download a view csv
    ts.download_view_csv('usage_test','Sheet 1')
    #ts.download_view_csv('INNOVATION','INNOVATION STYLES')
if __name__ == '__main__':
    main()
