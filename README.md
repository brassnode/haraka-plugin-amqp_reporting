[![CI Test Status][ci-img]][ci-url]
[![Code Climate][clim-img]][clim-url]

# haraka-plugin-amqp_reporting

Clone me, to create a new Haraka plugin!

## Template Instructions

These instructions will not self-destruct after use. Use and destroy.

See also, [How to Write a Plugin](https://github.com/haraka/Haraka/wiki/Write-a-Plugin) and [Plugins.md](https://github.com/haraka/Haraka/blob/master/docs/Plugins.md) for additional plugin writing information.

## Create a new repo for your plugin

Haraka plugins are named like `haraka-plugin-something`. All the namespace after `haraka-plugin-` is yours for the taking. Please check the [Plugins](https://github.com/haraka/Haraka/blob/master/Plugins.md) page and a Google search to see what plugins already exist.

Once you've settled on a name, create the GitHub repo. On the [amqp_reporting repo's main page](https://github.com/haraka/haraka-plugin-amqp_reporting), click the _Use this amqp_reporting_ button and create your new repository. Then paste that URL into a local ENV variable with a command like this:

```sh
export MY_GITHUB_ORG=haraka
export MY_PLUGIN_NAME=haraka-plugin-SOMETHING
```

Clone and rename the amqp_reporting repo:

```sh
git clone git@github.com:haraka/$MY_GITHUB_ORG/$MY_PLUGIN_NAME.git
cd $MY_PLUGIN_NAME
```

Now you'll have a local git repo to begin authoring your plugin

## rename boilerplate

Replaces all uses of the word `amqp_reporting` with your plugin's name.

./redress.sh [something]

You'll then be prompted to update package.json and then force push this repo onto the GitHub repo you've created earlier.

# Add your content here

## INSTALL

```sh
cd /path/to/local/haraka
npm install haraka-plugin-amqp_reporting
echo "amqp_reporting" >> config/plugins
service haraka restart
```

### Configuration

If the default configuration is not sufficient, copy the config file from the distribution into your haraka config dir and then modify it:

```sh
cp node_modules/haraka-plugin-amqp_reporting/config/amqp_reporting.ini config/amqp_reporting.ini
$EDITOR config/amqp_reporting.ini
```

## USAGE

<!-- leave these buried at the bottom of the document -->

[ci-img]: https://github.com/haraka/haraka-plugin-amqp_reporting/actions/workflows/ci.yml/badge.svg
[ci-url]: https://github.com/haraka/haraka-plugin-amqp_reporting/actions/workflows/ci.yml
[clim-img]: https://codeclimate.com/github/haraka/haraka-plugin-amqp_reporting/badges/gpa.svg
[clim-url]: https://codeclimate.com/github/haraka/haraka-plugin-amqp_reporting
