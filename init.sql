CREATE DATABASE testplanet CHARACTER SET utf8 COLLATE utf8_general_ci;
create table testplanet.session
(
  id       int auto_increment
    primary key,
  client   int          null,
  crc      bigint       null,
  elite    bigint       null,
  visit_in datetime     null,
  time     varchar(256) null,
  duration int          null
);