# buddy-plugin-template

This is an initial template of a plugin for [Buddy](https://github.com/manticoresoftware/manticoresearch-buddy/) that you can use to develop.

## How to start

1. Create a repository from this template.
2. Open `composer.json` and edit it with your plugin name, dependencies, and everything else you need.
3. Run `composer install` to make it work.
4. You need to use our official `manticore-executor`; we strictly recommend using the [Docker](#how-to-use-docker-of-manticore-executor-dev) flow.
5. Edit `src/Payload.php`, add logic to parse the request, and prepare the payload structure for your command processing.
6. Edit `src/Handler.php` and implement processing logic and response that will be proxied by Manticore to the client.

## How to use Docker of manticore-executor-dev

1. Clone [Buddy](https://github.com/manticoresoftware/manticoresearch-buddy/) somewhere to your host machine and go into it.
2. Run this command to create a Docker image named `manticore-buddy`:

    ```bash
    docker run --privileged --entrypoint bash -v $(pwd):/workdir -w /workdir --name manticore-buddy  --network host -it manticoresearch/manticore-executor-kit:latest
    ```

3. Now you can edit `/etc/manticore.conf` and configure the development version of Buddy:

    ```text
    buddy_path = manticore-executor-dev /workdir/src/main.php --debug
    ```

4. Run `searchd --nodetach` to keep the daemon in the foreground and enjoy the development of your new plugin.

## Notes

You can use `pre-commit` to install code style and code analyze hooks by running `pre-commit install`. Check our PHP code style and guide [here](https://github.com/manticoresoftware/php-code-standard).
