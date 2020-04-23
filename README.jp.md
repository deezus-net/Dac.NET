![](https://github.com/deezus-net/Dac.Net/workflows/test/badge.svg)
# DAC
DAC(Database As Codeの略)はデータベースのテーブル構造をyamlで管理するツールです  

## 使い方
```
dac [コマンド] [オプション]
```

## コマンド
|コマンド| 説明 |
|:---|:---|
| extract | データベースに接続しテーブルをymlとしてエクスポートします|
| create | yamlを元にテーブルを作成します |
| recreate| 既存テーブルをdropし、yamlを元にテーブルを再構成します |
| update | yamlとデータベースの差分を比較し、テーブルやカラムを更新します |
| diff | yamlとデータベースの差分を表示します |

## オプション
| オプション | 説明 | 例など | |
|:---|:---|:---|:---:|
| --hosts &lt;hosts&gt;| データベースへの接続情報ymlファイルパス | hosts.yml | |
| --host &lt;host&gt; | データベースホスト(--hosts指定時は接続先名)|localhost| * |
| --type &lt;type&gt; | データベースタイプ | mysql, pgsql, mssql | * |
| --user &lt;user&gt; | データベースに接続するユーザー |  | * |
| --password &lt;password&gt; | データベースに接続する際のパスワード |  | * |
| --database &lt;database&gt; | データベース名 | | * |
| --input &lt;input&gt; | 入力yamlファイルパス | db.yml | |
| --output &lt;output&gt; | extract時の出力先ディレクトリ | | |
| --query | create, recreate, update 時にクエリを実行せずに画面に出力します| | |
| --dry-run | create, recreate, update 時にクエリを実行しますが、コミットしません | | 
| --help | ヘルプ表示| |

※--hosts未指定の場合は*のオプションが必須です

## 使用例

### extract
引数で接続情報を指定する場合
```
dac extract --host localhost --type mysql --user root --password password --database dac -o .
```
ファイルで接続情報を設定する場合
```
dac extract --hosts hosts.yml --output .
```
------------
  
### create
引数で接続情報を指定する場合
```
dac create --host localhost --type mysql --user root --password password --database dac --input db.yml
```
ファイルで接続情報を設定する場合
```
dac create --hosts hosts.yml --input db.yml
```
クエリを表示する場合
```
dac create --hosts hosts.yml --input db.yml --query
```
------------
  
### recreate
引数で接続情報を指定する場合
```
dac recreate --host localhost --type mysql --user root --password password --database dac --input db.yml
```
ファイルで接続情報を設定する場合
```
dac recreate --hosts hosts.yml --input db.yml
```
クエリを表示する場合
```
dac recreate --hosts hosts.yml --input db.yml --query
```
------------
  
### update
引数で接続情報を指定する場合
```
dac update --host localhost --type mysql --user root --password password --database dac --input db.yml
```
ファイルで接続情報を設定する場合
```
dac update --hosts hosts.yml --input db.yml
```
クエリを表示する場合
```
dac update --hosts hosts.yml --inpit db.yml --query
```
------------
  
### diff
引数で接続情報を指定する場合
```
dac diff --host localhost --type mysql --user root --password password --database dac --input db.yml
```
ファイルで接続情報を設定する場合
```
dac diff --hosts hosts.yml --input db.yml
```

## hostsに関して
yamlで接続情報を複数記載することができます  
下記のように種類の違うデータベースを混在させることもできます  
--hostオプションで名前を指定しない場合は全接続先に対してコマンドが実行されます
```yaml:hosts.yml
server1:
  type: mysql
  host: localhost
  user: db_user_1
  password: password
  database: dac
 
server2:
  type: pgsql
  host: localhost
  user: db_user_2
  password: password
  database: dac

server3:
  type: mssql
  host: localhost
  user: sa
  password: !Passw0rd
  database: dac
```

### extractの例
全接続先に対して行う場合
server1.yml, server2.yml, server3.ymlと接続先ごとにファイルが作成されます
```
dac extract --hosts hosts.yml --output .
```

接続先名を指定する場合
server1のみをextractし、server1.ymlが作成されます
```
dac extract --hosts hosts.yml --host server1 --output .

```