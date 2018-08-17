import pandas as pd
a = [1,2,3,4,5,6]
v = ['a','v',',','as','as','lll']
df = pd.DataFrame({'joj':a, 'meow':v})
df.to_csv('dags/csv/test.csv')
