--
-- PostgreSQL database dump
--

-- Dumped from database version 10.18
-- Dumped by pg_dump version 10.18

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Data for Name: continent; Type: TABLE DATA; Schema: public; Owner: sormas_user
--

COPY public.continent (id, uuid, creationdate, changedate, archived, defaultname, externalid) FROM stdin;
113	SQQZKE-SVPKSR-S3HHQL-TSUJCL64	2021-05-12 12:00:04.157787	2021-05-12 12:00:04.157	f	Africa	31000005
114	TFVBTA-DHAU5I-HR5K2E-7IHRSJSI	2021-05-12 12:00:04.190137	2021-05-12 12:00:04.19	f	America	31000006
115	TP4WPS-W7UIJ5-7KT4SY-FBYE2E5A	2021-05-12 12:00:04.202338	2021-05-12 12:00:04.202	f	Asia	31000004
116	TGXMPS-D2RJH2-WKWOOX-QB3O2ORQ	2021-05-12 12:00:04.216839	2021-05-12 12:00:04.217	f	Australia (Continent)	31000007
117	XX3JAO-OLM7K7-OWA2DE-JQHPKDNU	2021-05-12 12:00:04.236076	2021-05-12 12:00:04.236	f	Europe	31000003
118	ROJ3SJ-CYGWMC-55DLOH-7ME4SE4U	2021-05-12 12:00:04.249745	2021-05-12 12:00:04.249	f	Foreign Countries (Unknown)	31099999
\.


--
-- PostgreSQL database dump complete
--

