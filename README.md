Million Song Database - Postings List Generator
===============================================

Creates a postings list from [lastfm's million song database](http://labrosa.ee.columbia.edu/millionsong/lastfm)

Building
--------
`mvn install`

Testing
-------
`mvn test`

Mapper
------

Maps `track_id|lastfm_id|artist|track|num_tags|tag1,score1|tag2,score2...` to key `tag1` and value `track_id,score1` and key `tag2` and value `track_id,score2` ...

Reducer
-------

Concatenates key `tag` and values `track_id,score` to `tag|track_id1,score1|track_id2,score2...`