# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'scalenav'
copyright = '2024, Ivann Schlosser'
author = 'Ivann Schlosser'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ['sphinx.ext.autodoc',
              'sphinx.ext.intersphinx',
              # 'myst_parser',
              'sphinx.ext.napoleon',
            #   "nbsphinx",
              'myst_nb',         
              'sphinx.ext.autosummary',
               ]

templates_path = ['_templates']
exclude_patterns = []

# Napoleon settings
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = False
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = True
napoleon_use_admonition_for_examples = False
napoleon_use_admonition_for_notes = False
napoleon_use_admonition_for_references = False
napoleon_use_ivar = False
napoleon_use_param = True
napoleon_use_rtype = True
napoleon_preprocess_types = False
napoleon_type_aliases = None
napoleon_attr_annotations = True


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'furo'
html_static_path = ['_static']
# html_baseurl = ''
html_theme_options = {
    # "github_url": "https://github.com/nismod/scale-nav",
    # "repository_url": "https://github.com/nismod/scale-nav",
    # "repository_branch": "master",
    # "home_page_in_toc": True,
    # "path_to_docs": "docs",
    # "show_navbar_depth": 1,
    # "use_edit_page_button": True,
    # "use_repository_button": True,
    # "use_download_button": True,
    # "launch_buttons": {
    #     "binderhub_url": "https://mybinder.org",
    #     "notebook_interface": "classic",
    # },
    # "navigation_with_keys": False,
}


source_suffix = {'.md': 'markdown', '.rst': 'restructuredtext', '.ipynb': 'myst-nb'}
# source_suffix = [".md",".rst",".ipynb"]

nb_execution_mode = "off" # "cache"   

# Automatically extract typehints when specified and place them in
# descriptions of the relevant function/method.
autodoc_typehints = "description"

# Don't show class signature with the class' name.
autodoc_class_signature = "separated"