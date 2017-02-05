# download pageviews on background
nohup wget https://dumps.wikimedia.org/other/pageviews/2015/2015-05/ &

# avoid downloading index.html file in each page 
nohup wget -r --no-parent --reject "index.html*" https://dumps.wikimedia.org/other/pageviews/2015/2015-05/ &

# pagelinks sql dump
nohup wget -r --no-parent --reject "index.html*" https://dumps.wikimedia.org/enwiki/20170101/enwiki-20170101-pagelinks.sql.gz  &

# pages sql dump
nohup wget -r --no-parent --reject "index.html*" https://dumps.wikimedia.org/enwiki/20170101/enwiki-20170101-page.sql.gz   &
