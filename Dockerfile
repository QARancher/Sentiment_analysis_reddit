FROM python:3.9.7-slim-buster as base

# Setup env
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONFAULTHANDLER 1
ENV PATH=/home/reddit/.local/bin:$PATH
ENV FT_APP_ENV="docker"

RUN mkdir /home/reddit \
    && useradd -u 1000 -G sudo -U -m -s /bin/bash reddit \
    && chown reddit:reddit /home/reddit \
    # Allow sudoers
    && echo "reddit ALL=(ALL) NOPASSWD: /bin/chown" >> /etc/sudoers
RUN pip install --upgrade pip
WORKDIR /home/reddit

FROM base as python-deps
ENV LD_LIBRARY_PATH /usr/local/lib
COPY --chown=reddit:reddit requirements.txt /home/reddit
USER reddit

RUN pip install -r requirements.txt

# Copy dependencies to runtime-image
FROM base as runtime-image
COPY --from=python-deps /usr/local/lib /usr/local/lib
ENV LD_LIBRARY_PATH /usr/local/lib

COPY --from=python-deps --chown=reddit:reddit /home/reddit/.local /home/reddit/.local

USER reddit
COPY --chown=reddit:reddit ./* /home/reddit/


# Default to trade mode
