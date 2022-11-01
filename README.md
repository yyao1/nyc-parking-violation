# nyc-parking-violation
This project analyzed a dataset containing 60millions of NYC parking violations since 2012. By completing this project, it demonstrats the mastery of principles of containerization, terminal navigation, python scripting, and AWS EC2 provisioning.
From the visuals of Kibana, it shows that NY has the most violation counts among all the counties. And the most frequent violation rule is 'No parking-day/time limits'

Build Docker:
`docker build -t bigdata1:1.0`

Run Docker:
`sudo docker run -e APP_KEY=YPlAFn2V5jvM1HPWXlkgJmW8R -t yyao1/bigdata1:1.0 python main.py --page_size=1000` 
