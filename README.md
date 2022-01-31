# EAQuery
Tableau CRMデータセットにクエリをサブミットするAPI client。
**現行バージョンではsandboxには対応していません！**
## installation
eaqディレクトリで`pip install .`でインストールしてください。

## Examples
username, passwordはハードコードせず、**setting.ini**などで管理しましょう。
```
from eaquery import EAQuery

eaq = EAQuery(username = "myemail@example.com",
              password = "password"
              )
```

REST APIのバージョンを指定する場合は以下のように指定する。
```
eaq = EAQuery(username = "myemail@example.com",
              password = "password",
              version = 53
              )
```
### SAQL
```
# SAQLコード内のクエリ変数は必ず"q"にすること("foreach q generate"にする)
q = """
foreach q generate 'Account.AnnualRevenue' as 'Account.AnnualRevenue',
                       'Account.NumberOfEmployees' as 'Account.NumberOfEmployees', 
                       'Account.Type' as 'Account.Type',
                       'AccountId' as 'AccountId', 
                       'FiscalQuarter' as 'FiscalQuarter', 
                       'FiscalYear' as 'FiscalYear', 
                       'ForecastCategory' as 'ForecastCategory', 
                       'HasOpportunityLineItem' as 'HasOpportunityLineItem', 
                       'Imple3__c' as 'Imple3__c', 
                       'f_Amount_Consulting__c' as 'f_Amount_Consulting__c'
                       ;
"""
data = "Opp_EA"
# 第一引数に必ずクエリ対象のTableau CRM data名を指定する
df = eaq.read_saql(data, q)
```

## SQL
```
# pandasのread_sqlと同じような使用方法
q2 = """SELECT COUNT(*) as "A" FROM "Opp_EA" ;"""
df2 = eaq.read_sql(q2)
```

## Recipeのjson定義ファイル取得
```
recipes = eaq.get_all_recipe()
```
すべてのレシピ情報をリスト型で返します。
リストの各値はdictionary形式で
```
{
    "id": "レシピid", 
    "label":"レシピのラベル名": 
    "recipe":"json定義"
}
```
の形式で格納されています。