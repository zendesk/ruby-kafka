FROM ruby:2.2

# throw errors if Gemfile has been modified since Gemfile.lock
RUN bundle config --global frozen 1

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY ruby-kafka.gemspec /usr/src/app/
COPY Gemfile /usr/src/app/
COPY Gemfile.lock /usr/src/app/
COPY VERSION /usr/src/app/

RUN bundle install

ADD lib/ /usr/src/app/lib
ADD spec/ /usr/src/app/spec

CMD ["echo hello"]
