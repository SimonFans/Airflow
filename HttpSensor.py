from airflow.sensors.http_sensor import HttpSensor

is_forex_rates_available = HttpSensor(
        task_id='is_forex_rates_available',
        method='GET',
        http_conn_id='forex_api',
        endpoint='latest',
        response_check=lambda response: "rates" in response.text,
        poke_interval=5,
        timeout=20
        
 # connection:
 
 conn_id:
 conn_type: http
 host: http://api.exchangeratesapi.io/
 
 # example:
 # https://api.exchangeratesapi.io/latest
 
 {"rates":{"CAD":1.5265,"HKD":8.4259,"ISK":155.9,"PHP":54.939,"DKK":7.4657,"HUF":354.76,"CZK":26.909,"AUD":1.7444,"RON":4.833,"SEK":10.9455,"IDR":17243.21,"INR":82.9275,"BRL":5.5956,"RUB":80.69,"HRK":7.6175,"JPY":118.33,"THB":35.665,"CHF":1.0558,"SGD":1.5479,"PLN":4.5586,"BGN":1.9558,"TRY":7.3233,"CNY":7.6709,"NOK":11.2143,"NZD":1.8128,"ZAR":19.6383,"USD":1.0867,"MXN":26.0321,"ILS":3.8919,"GBP":0.87565,"KRW":1322.49,"MYR":4.7136},"base":"EUR","date":"2020-04-09"}
