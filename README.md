# cola - Cache Oblivious Lookahead Array
Copyright (c) 2013 Gianni Tedesco

---

## INTRODUCTION
This implements the COLA structure described in the paper "Cache Oblivious
Streaming B-Trees" by Bender, Farach-Colton, et al.

We use mmap where possible and do k-way merges using a binary min-heap instead
of binary merges.

## NOT IMPLEMENTED
1. Values are not stored yet.
2. No fractional cascading or any other optimisation of queries.
3. Deamortisation (via background write thread) is also not implemented.

## BUILDING
 $ make

## RUNNING
 $ ./cola help

If you like and use this software then press [<img src="http://www.paypalobjects.com/en_US/i/btn/btn_donate_SM.gif">](https://www.paypal.com/cgi-bin/webscr?cmd=_donations&business=gianni%40scaramanga%2eco%2euk&lc=GB&item_name=Gianni%20Tedesco&item_number=scaramanga&currency_code=GBP&bn=PP%2dDonationsBF%3abtn_donateCC_LG%2egif%3aNonHosted) to donate towards its development progress and email me to say what features you would like added.
