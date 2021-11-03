psql -c "COPY (select * from "AnalogTransition" where "Timestamp" > '2021-09-01' and "Timestamp" < '2021-09-30') TO STDOUT;" -h localhost -d my_database -U my_user > path/to/file



pg_dump -U postgres -h 10.0.100.100 -p 5433 -t "DataPointDetail" "ADMIN\Visual T&D Server\HMI" > "D:\_temp\DataPointDetail.sql"



psql -U postgres -c "COPY (select * from \"DataPointDetail\" ) TO STDOUT;" -h 10.0.100.100 -p 5433 -d "ADMIN\Visual T&D Server\HMI" > "D:\_temp\DataPointDetail.csv"


psql -U postgres -c "COPY (select * from \"Tag\" ) TO STDOUT;" -h 10.0.100.100 -p 5433 -d "ADMIN\Visual T&D Server\HMI" > "D:\_temp\Tag.csv"


pg_dump -U postgres -h 10.0.100.100 -p 5433 -t "\"AnalogTransitions\"" "ADMIN\Visual T&D Server\HMI" > "D:\_temp\AnalogTransitions.sql"


pg_dump -U postgres -h localhost -p 5433 "DESKTOP-LKMFART\Visual T&D Server\HMI_DUMP" > "D:\_temp\Meachan20210823_0929.sql"


psql -U postgres -h localhost -p 5433 -d "DESKTOP-LKMFART\Visual T&D Server\HMI_DUMP" -f "D:\_temp\MeaChan20210929.sql"

./psql -U postgres -h localhost -p 5433 -d "DESKTOP-50AQ6VK\Visual T&D Server\MeaChan\20210823" -f "/Volumes/SeagateEX/Meachan20210823_0929.sql"

