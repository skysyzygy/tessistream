if (interactive()) {
  require("devtools")
}

# if rprojroot package is available
if (system.file(package = "rprojroot") != "") {
  Sys.setenv(TAR_CONFIG = rprojroot::find_package_root_file("inst", "extdata", "_targets.yaml"))
  Sys.setenv(R_CONFIG_FILE = rprojroot::find_package_root_file("tests", "tessistream-config.yml"))
}
