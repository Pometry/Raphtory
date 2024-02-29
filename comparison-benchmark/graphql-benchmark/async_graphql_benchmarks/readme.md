# how to run 

## first build the starwars server

> git clone https://github.com/async-graphql/async-graphql
> cd async-graphql
> git clone https://github.com/async-graphql/examples/
> cd examples/poem/starwars
> cargo run -r 


## Run the benchmark via artillery 

### setup 

> npm install -g artillery 
> artillery run artillery_starwars_benchmark.yml

### results

    errors.ECONNRESET: ............................................................. 49
    errors.EPIPE: .................................................................. 8
    errors.ETIMEDOUT: .............................................................. 300
    http.codes.200: ................................................................ 492
    http.downloaded_bytes: ......................................................... 21648
    http.request_rate: ............................................................. 550/sec
    http.requests: ................................................................. 5499



## Run the benchmark via locust 

### setup

> pip install locust 
> locust -f locust_starwars_benchmark.py --host=http://localhost:8000
>  Go into the locust browser http://0.0.0.0:8089/
>  Select 1000 users, 100 ramp up, press start
>  Wait for it to start failing 


### result

    Requests 1520, fails 12, average ms 144.7, current failurs 0.75, current rps 138



/etc/systemd/system.conf
/etc/systemd/user.conf
    > DefaultLimitNOFILE=500000

/etc/pam.d/common-session
    > session required pam_limits.so

/etc/security/limits.conf
    *         hard    nofile      500000
    *         soft    nofile      500000
    root      hard    nofile      500000
    root      soft    nofile      500000


