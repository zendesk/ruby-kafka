FROM ruby:2.2

# throw errors if Gemfile has been modified since Gemfile.lock
RUN bundle config --global frozen 1

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY ruby-kafka.gemspec /usr/src/app/
COPY Gemfile /usr/src/app/
COPY Gemfile.lock /usr/src/app/

ADD lib/ /usr/src/app/lib
ADD spec/ /usr/src/app/spec

RUN bundle install

CMD ["echo hello"]
