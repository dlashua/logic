# Goal Types

## Standard Goal

input: `input$` (`SimpleObservable<Subst>`)

returns: `output$` (`SimpleObservable<Subst>`)

signature:

```typescript
(...args) =>
  (input$: SimpleObservable<Subst>) =>
    SimpleObservable<Subst>;
```

output$ can emit more than it received, less than it received, equal, or not at all

Useful for:

- Aggregates
- Membero
- Or
- Low Level Goals

Requires:

- walk to get possible Term Values
- unify to set New Values

## Subst Goal ??

input: `inputSubst` (`Subst`)

returns: `outputSubst` (`Subst`)

signature:

```typescript
(...args) =>
    SubstGoal(
        (inputSubst: Subst) =>
            Subst | Fail | Suspend<...args>
    )
```

Useful for:

- 1 to 1 Mappings
- 1 to 0 Mappings
- Simple Constraints (lteo, take, etc)

Requires:

- walk to get possible Term Values
- unify to set New Values

## Walked Goal ??

input: `walkedSubst` (`Subst`)

just like `Subst Goal`, except all the terms are pre walked

Requires:

- unify to set New Values

signature:

```javascript
(...args) =>
    WalkedGoal(
        (inputSubst: Subst, ...walkedArgs) =>
            Subst | Fail | Suspend<...args>
    )
```

## Lifted Goal

signature:

```javascript
(...args, outVar) =>
    lift(
        (...walkedArgs) => Any | Fail
    )
```

Features:

- takes a regular javascript function.
- automatically adds a term to the end of the argument list to accept the output.
- Walks all terms
- does not yield if all terms are not grounded

## Suspendable Lifted Goal

signature:

```javascript
(...args, outVar) =>
    suspendableLift(
        (...walkedArgs) =>
            Any | Fail | Suspend<...args>
    )
```

Features:

- Similar to `Lifted Goal`
- Suspends until all terms are grounded

# Special Relations

## Subquery

signature:

```javascript
Subquery(select, where, [aggregator], outVar);
```
