package io.fullerstack.substrates;

import io.fullerstack.substrates.FsOperators.Wrap;
import io.humainary.substrates.api.Substrates.Subject;

import java.util.function.Consumer;
import java.util.function.Function;

/// **Recipe** — the immutable wiring plan for one operator chain.
///
/// A `Flow` or `Fiber` is a plan, not a chain. It is composed once, shared freely, and
/// **materialised** against each target it is attached to, every materialisation producing
/// independent state. This is the plan.
///
/// ## Why a structure and not an array
///
/// The plan grows by composition — `flow.map(f).clamp(lo, hi)` — and it is immutable, so every
/// step produces a new plan. Held as an array, each step must copy the whole array; held as a
/// tree, each step is one node that **shares everything before it**. Composition becomes O(1)
/// and allocates one object instead of copying n references.
///
/// It also removes a flattening step that never had to exist. Three of the four cases below are
/// *nested plans* — a fiber inlined into a flow, a per-attachment factory — and the array form
/// had to splice them into one flat sequence at materialisation time, tracking positions across
/// a `Wrap[][]` with six array copies and a manual write cursor. A tree needs no flattening: a
/// nested plan is a node, and the fold walks into it.
///
/// ## Order
///
/// Nodes are held **newest-first**: the most recently composed operator is at the head. That is
/// the order [#wire] consumes them in, because wiring runs backwards — the last operator wraps
/// the target, and each earlier operator wraps that. Walking a newest-first list forwards is
/// therefore walking the pipeline backwards, which is exactly what wiring wants — no reversal
/// step, and no position arithmetic. Wiring recurses along the spine, which is bounded by the
/// number of composed operators and happens once per attachment, never per emission.
///
/// A worked example. `flow.map(f).clamp(lo, hi)` holds:
///
/// ```
/// Stage ( clamp, Stage ( map, Empty ) )
/// ```
///
/// and wiring against `target` accumulates `clamp.wrap ( target )`, then
/// `map.wrap ( clamp.wrap ( target ) )` — so `map` is outermost and sees each value first,
/// which is the order it was composed in.
sealed interface Recipe {

  /// The empty plan — an identity chain.
  ///
  /// A flow holding this materialises to its target unchanged, which is what lets
  /// `Flow.pipe` return the target pipe directly rather than wrapping it in a chain that does
  /// nothing.
  Recipe EMPTY = new Empty ();

  /// Builds the consumer chain for one attachment.
  ///
  /// `subject` is the target's subject, needed only by [Deferred] — the §6.2 contract is that a
  /// per-attachment factory is invoked once per attachment, against the subject it is being
  /// attached to.
  ///
  /// @param subject    the subject this chain is being attached to
  /// @param downstream what the last operator in the plan feeds
  /// @return the consumer the first operator in the plan is fronted by
  Consumer < Object > wire (
    Subject < ? > subject,
    Consumer < Object > downstream
  );

  /// Wires a plan that carries no per-attachment factory.
  ///
  /// A `Fiber`'s plan is one by construction: the `Fiber` API has no factory-taking operator,
  /// so a [Deferred] can never reach one. That invariant is what lets `when(predicate, fiber)`
  /// materialise its sub-fiber inside `wrap`, where no target subject exists yet.
  ///
  /// [Deferred] rejects this call rather than inventing a subject, so a plan that breaks the
  /// invariant says so instead of failing obscurely later.
  default Consumer < Object > wire ( Consumer < Object > downstream ) {
    return wire ( null, downstream );
  }

  /// Whether this plan contributes no operators, so materialisation can be elided entirely.
  ///
  /// A [Deferred] is never empty even when the factory might yield an empty plan: the factory
  /// has to run to find out, and §6.2 says it runs once per attachment, so eliding it here
  /// would skip a call the contract promises.
  boolean isEmpty ();

  /// This plan with `op` composed after everything already in it. O(1).
  default Recipe then ( Wrap < ? > op ) {
    return new Stage ( op, this );
  }

  /// This plan with `inner` composed after everything already in it. O(1).
  ///
  /// Used where a fiber is inlined into a flow, or one fiber extends another — the nested plan
  /// stays nested rather than being spliced into a flat sequence.
  default Recipe then ( Recipe inner ) {
    return inner.isEmpty () ? this : new Splice ( inner, this );
  }

  /// This plan with a per-attachment plan composed after everything already in it. O(1).
  ///
  /// `resolve` is called once per materialisation, against the target's subject. Named apart
  /// from the `then` overloads because [Wrap] and `Function` are both single-argument
  /// functional interfaces, so a lambda would match either.
  default Recipe thenResolving ( Function < Subject < ? >, Recipe > resolve ) {
    return new Deferred ( resolve, this );
  }

  /// The identity plan.
  record Empty () implements Recipe {

    @Override
    public Consumer < Object > wire ( Subject < ? > subject, Consumer < Object > downstream ) {
      return downstream;
    }

    @Override
    public boolean isEmpty () {
      return true;
    }
  }

  /// One operator, composed after `rest`.
  record Stage ( Wrap < ? > op, Recipe rest ) implements Recipe {

    @Override
    @SuppressWarnings ( { "unchecked", "rawtypes" } )
    public Consumer < Object > wire ( Subject < ? > subject, Consumer < Object > downstream ) {
      // Newest-first, so `rest` is everything BEFORE this operator in the pipeline and must
      // end up wrapping it — hence rest wires around what this operator produces.
      return rest.wire ( subject, ( (Wrap) op ).wrap ( downstream ) );
    }

    @Override
    public boolean isEmpty () {
      return false;
    }
  }

  /// A nested plan, composed after `rest`. The fold walks into it; nothing is flattened.
  record Splice ( Recipe inner, Recipe rest ) implements Recipe {

    @Override
    public Consumer < Object > wire ( Subject < ? > subject, Consumer < Object > downstream ) {
      return rest.wire ( subject, inner.wire ( subject, downstream ) );
    }

    @Override
    public boolean isEmpty () {
      return inner.isEmpty () && rest.isEmpty ();
    }
  }

  /// A plan produced per attachment, composed after `rest`.
  ///
  /// §6.2: the factory is invoked once per attachment, against the target's subject. That is
  /// exactly when [#wire] runs, so there is nothing to schedule — resolving is a step of the
  /// fold.
  record Deferred ( Function < Subject < ? >, Recipe > resolve, Recipe rest ) implements Recipe {

    @Override
    public Consumer < Object > wire ( Subject < ? > subject, Consumer < Object > downstream ) {
      if ( subject == null ) {
        throw new IllegalStateException (
          "a per-attachment factory can only be wired against a target subject" );
      }
      final Recipe resolved = resolve.apply ( subject );
      return rest.wire ( subject, resolved.wire ( subject, downstream ) );
    }

    @Override
    public boolean isEmpty () {
      return false;
    }
  }
}
