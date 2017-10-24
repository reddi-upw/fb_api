# fb_api
Some examples of using fb api

Usage
-----

```shell
$ cd fb_api
$ pip install -r requirements.txt
$ python -m scripts.page_sats -t '<access_token>' -q '<page_name>' # find page by <page_name>
$ python -m scripts.group_sats -t '<access_token>' -q '<group_name>' # find group by <group_name>
$ python -m scripts.page_sats -t '<access_token>' -p '<page_id>'  # find page by <page_id>
$ python -m scripts.group_sats -t '<access_token>' -g '<group_id>' # find group by <group_id>
$ python -m scripts.group_sats -t '<access_token>' -g '<group_id>' -o '<out.json>' # redirect output to <out.json>
```
