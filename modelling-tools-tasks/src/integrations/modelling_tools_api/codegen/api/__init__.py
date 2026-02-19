# flake8: noqa

if __import__("typing").TYPE_CHECKING:
    # import apis into api package
    from src.integrations.modelling_tools_api.codegen.api.internal_api import InternalApi
    from src.integrations.modelling_tools_api.codegen.api.v1_api import V1Api
    
else:
    from lazy_imports import LazyModule, as_package, load

    load(
        LazyModule(
            *as_package(__file__),
            """# import apis into api package
from src.integrations.modelling_tools_api.codegen.api.internal_api import InternalApi
from src.integrations.modelling_tools_api.codegen.api.v1_api import V1Api

""",
            name=__name__,
            doc=__doc__,
        )
    )
