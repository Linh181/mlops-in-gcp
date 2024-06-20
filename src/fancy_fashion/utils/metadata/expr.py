class Expr:
    """Base class representing an expression in Googles ML metadata filter language."""

    def __and__(self, other: "Expr") -> "Expr":
        return And(self, other)

    def __or__(self, other: "Expr") -> "Expr":
        return Or(self, other)


class Equals(Expr):
    """Expression that checks for quality of an attribute with a given value."""

    def __init__(self, attr: str, value: str):
        self._attr = attr
        self._value = value

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{self._attr}', '{self._value}')"

    def __str__(self) -> str:
        return f'{self._attr}="{self._value}"'


class InContext(Expr):
    """Expression that checks if an object is within the given context."""

    def __init__(self, context: str):
        self._context = context

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{self._context}')"

    def __str__(self) -> str:
        return f'in_context("{self._context}")'


class HasParentContext(Expr):
    """Expression that checks if an object has a given parent context."""

    def __init__(self, context: str):
        self._context = context

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{self._context}')"

    def __str__(self) -> str:
        return f'parent_contexts: "{self._context}"'


class And(Expr):
    """Expression that applies an AND to the given expressions."""

    def __init__(self, *exprs: Expr):
        self._exprs = exprs

    def __str__(self) -> str:
        return f"({' AND '.join(str(expr) for expr in self._exprs)})"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}{self._exprs}"


class Or(Expr):
    """Expression that applies an OR to the given expressions."""

    def __init__(self, *exprs: Expr):
        self._exprs = exprs

    def __str__(self) -> str:
        return f"({' OR '.join(str(expr) for expr in self._exprs)})"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}{self._exprs}"


def equals(attr: str, value: str) -> Equals:
    """Expression that checks for equality of an attribute with a given value."""
    return Equals(attr, value)


def in_context(context: str) -> InContext:
    """Expression that filters for objects within the given context."""
    return InContext(context)


def has_parent_context(parent_context: str) -> Expr:
    """Expression that filters for objects with a given parent context."""
    return HasParentContext(parent_context)


def schema_title(name: str) -> Expr:
    """Expression that filters for objects with the given schema title."""
    return equals("schema_title", name)


def metastore_context(project_id: str, region: str, metadata_store: str) -> str:
    """Generates the context ID for a ametastore in the given project/region."""
    return f"projects/{project_id}/locations/{region}/metadataStores/{metadata_store}"


def pipeline_context(project_id: str, region: str, metadata_store: str, pipeline_name: str) -> str:
    """Generates the context ID for a pipeline in the given project/region/store."""
    return (
        f"projects/{project_id}/locations/{region}/"
        f"metadataStores/{metadata_store}/contexts/{pipeline_name}"
    )


def run_context(project_id: str, region: str, metadata_store: str, run_name: str) -> str:
    """Generates the context ID for a pipeline run in the given project/region/store."""
    return (
        f"projects/{project_id}/locations/{region}/"
        f"metadataStores/{metadata_store}/contexts/{run_name}"
    )
