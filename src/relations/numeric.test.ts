import { describe, it, expect, beforeEach } from "vitest";
import { lvar, resetVarCounter } from "../core/kernel.ts";
import { eq, and, or, run } from "../core/combinators.ts";
import { query } from "../query.ts";
import { SimpleObservable } from "../core/observable.ts";
import { SUSPENDED_CONSTRAINTS } from "../core/subst-suspends.ts";
import {
  gto,
  lto,
  gteo,
  lteo,
  pluso,
  multo,
  maxo,
  mino,
  dividebyo,
  minuso,
} from "./numeric.ts";

describe("Numeric Relations", () => {
  beforeEach(() => {
    resetVarCounter();
  });

  describe("gto (greater than)", () => {
    it("should succeed when first value is greater than second", async () => {
      const results = await query()
        .select($ => ({
          passed: true 
        }))
        .where($ => gto(5, 3))
        .toArray();
      
      // Should have at least one solution
      expect(results.length > 0).toBe(true);
    });

    it("should fail when first value is not greater than second", async () => {
      const results = await query()
        .select($ => ({
          passed: true 
        }))
        .where($ => gto(3, 5))
        .toArray();
      
      // Should have no solutions
      expect(results.length === 0).toBe(true);
    });

    it("should fail when values are equal", async () => {
      const results = await query()
        .select($ => ({
          passed: true 
        }))
        .where($ => gto(5, 5))
        .toArray();
      
      // Should have no solutions
      expect(results.length === 0).toBe(true);
    });
  });

  describe("lto (less than)", () => {
    it("should succeed when first value is less than second", async () => {
      const results = await query()
        .select($ => ({
          success: true
        }))
        .where($ => lto(3, 5))
        .toArray();
      
      expect(results).toHaveLength(1);
      expect(results[0].success).toBe(true);
    });

    it("should fail when first value is not less than second", async () => {
      const results = await query()
        .select($ => ({
          success: true
        }))
        .where($ => lto(5, 3))
        .toArray();
      
      expect(results).toHaveLength(0);
    });
  });

  describe("gteo (greater than or equal)", () => {
    it("should succeed when first value is greater than second", async () => {
      const results = await query()
        .select($ => ({
          success: true
        }))
        .where($ => gteo(5, 3))
        .toArray();
      
      expect(results).toHaveLength(1);
      expect(results[0].success).toBe(true);
    });

    it("should succeed when values are equal", async () => {
      const results = await query()
        .select($ => ({
          success: true
        }))
        .where($ => gteo(5, 5))
        .toArray();
      
      expect(results).toHaveLength(1);
      expect(results[0].success).toBe(true);
    });
  });

  describe("lteo (less than or equal)", () => {
    it("should succeed when first value is less than second", async () => {
      const results = await query()
        .select($ => ({
          success: true
        }))
        .where($ => lteo(3, 5))
        .toArray();
      
      expect(results).toHaveLength(1);
      expect(results[0].success).toBe(true);
    });

    it("should succeed when values are equal", async () => {
      const results = await query()
        .select($ => ({
          success: true
        }))
        .where($ => lteo(5, 5))
        .toArray();
      
      expect(results).toHaveLength(1);
      expect(results[0].success).toBe(true);
    });
  });

  describe("pluso (addition constraint)", () => {
    it("should verify correct addition when all values are grounded", async () => {
      // Use query to test the actual constraint behavior
      const results = await query()
        .select($ => ({
          success: true
        }))
        .where($ => pluso(2, 3, 5))
        .toArray();
      
      expect(results).toHaveLength(1);
      expect(results[0].success).toBe(true);
    });

    it("should fail when addition is incorrect", async () => {
      const results = await query()
        .select($ => ({
          success: true
        }))
        .where($ => pluso(2, 3, 6))
        .toArray();
      
      expect(results).toHaveLength(0);
    });

    it("should compute z when x and y are grounded", async () => {
      const results = await query()
        .select($ => ({
          z: $.z
        }))
        .where($ => pluso(2, 3, $.z))
        .toArray();
      
      expect(results).toHaveLength(1);
      expect(results[0].z).toBe(5);
    });

    it("should compute y when x and z are grounded", async () => {
      const results = await query()
        .select($ => ({
          y: $.y
        }))
        .where($ => pluso(2, $.y, 7))
        .toArray();
      
      expect(results).toHaveLength(1);
      expect(results[0].y).toBe(5);
    });

    it("should compute x when y and z are grounded", async () => {
      const results = await query()
        .select($ => ({
          x: $.x
        }))
        .where($ => pluso($.x, 3, 8))
        .toArray();
      
      expect(results).toHaveLength(1);
      expect(results[0].x).toBe(5);
    });

    it("should handle constraint with insufficient variables and solve when more become available", async () => {
      // Test that constraints work even when initially there aren't enough variables
      const results = await query()
        .select($ => ({
          x: $.x,
          y: $.y,
          z: $.z
        }))
        .where($ => and(
          pluso($.x, $.y, $.z), // Initially all unbound
          eq($.x, 5), // Then bind x
          eq($.y, 3) // Then bind y, should compute z = 8
        ))
        .toArray();
      
      expect(results).toHaveLength(1);
      expect(results[0].x).toBe(5);
      expect(results[0].y).toBe(3);
      expect(results[0].z).toBe(8); // 5 + 3
    });

    it("should work with constraint chains", async () => {
      const results = await query()
        .select($ => ({
          x: $.x,
          y: $.y,
          z: $.z
        }))
        .where($ => and(
          pluso($.x, $.y, $.z),
          eq($.x, 4),
          eq($.y, 6)
        ))
        .toArray();
      
      expect(results).toHaveLength(1);
      expect(results[0].x).toBe(4);
      expect(results[0].y).toBe(6);
      expect(results[0].z).toBe(10);
    });

    it("should work with multiple constraint dependencies", async () => {
      const results = await query()
        .select($ => ({
          a: $.a,
          b: $.b,
          c: $.c,
          d: $.d
        }))
        .where($ => and(
          pluso($.a, $.b, $.c), // a + b = c
          pluso($.c, 2, $.d), // c + 2 = d
          eq($.a, 3),
          eq($.b, 4)
        ))
        .toArray();
      
      expect(results).toHaveLength(1);
      expect(results[0].a).toBe(3);
      expect(results[0].b).toBe(4);
      expect(results[0].c).toBe(7); // 3 + 4
      expect(results[0].d).toBe(9); // 7 + 2
    });
  });

  describe("minuso (addition constraint)", () => {
    it("should verify correct addition when all values are grounded", async () => {
      // Use query to test the actual constraint behavior
      const results = await query()
        .select($ => ({
          success: true
        }))
        .where($ => minuso(8, 4, 4))
        .toArray();
      
      expect(results).toHaveLength(1);
      expect(results[0].success).toBe(true);
    });

    it("should fail when addition is incorrect", async () => {
      const results = await query()
        .select($ => ({
          success: true
        }))
        .where($ => minuso(2, 3, 6))
        .toArray();
      
      expect(results).toHaveLength(0);
    });

    it("should compute z when x and y are grounded", async () => {
      const results = await query()
        .select($ => ({
          z: $.z
        }))
        .where($ => minuso(99,33, $.z))
        .toArray();
      
      expect(results).toHaveLength(1);
      expect(results[0].z).toBe(66);
    });

    it("should compute y when x and z are grounded", async () => {
      const results = await query()
        .select($ => ({
          y: $.y
        }))
        .where($ => minuso(9, $.y, 7))
        .toArray();
      
      expect(results).toHaveLength(1);
      expect(results[0].y).toBe(2);
    });

    it("should compute x when y and z are grounded", async () => {
      const results = await query()
        .select($ => ({
          x: $.x
        }))
        .where($ => minuso($.x, 3, 2))
        .toArray();
      
      expect(results).toHaveLength(1);
      expect(results[0].x).toBe(5);
    });

    it("should handle constraint with insufficient variables and solve when more become available", async () => {
      // Test that constraints work even when initially there aren't enough variables
      const results = await query()
        .select($ => ({
          x: $.x,
          y: $.y,
          z: $.z
        }))
        .where($ => and(
          minuso($.x, $.y, $.z), // Initially all unbound
          eq($.x, 5), // Then bind x
          eq($.y, 3) // Then bind y
        ))
        .toArray();
      
      expect(results).toHaveLength(1);
      expect(results[0].x).toBe(5);
      expect(results[0].y).toBe(3);
      expect(results[0].z).toBe(2); 
    });

    it("should work with constraint chains", async () => {
      const results = await query()
        .select($ => ({
          x: $.x,
          y: $.y,
          z: $.z
        }))
        .where($ => and(
          minuso($.x, $.y, $.z), 
          eq($.x, 6),
          eq($.y, 4)
        ))
        .toArray();
      
      expect(results).toHaveLength(1);
      expect(results[0].x).toBe(6);
      expect(results[0].y).toBe(4);
      expect(results[0].z).toBe(2);
    });

    it("should work with multiple constraint dependencies", async () => {
      const results = await query()
        .select($ => ({
          a: $.a,
          b: $.b,
          c: $.c,
          d: $.d
        }))
        .where($ => and(
          minuso($.a, $.b, $.c), // 27 - 4 = 23
          minuso($.c, 7, $.d), // 23 - 7 = 16
          eq($.a, 27),
          eq($.b, 4)
        ))
        .toArray();
      
      expect(results).toHaveLength(1);
      expect(results[0].a).toBe(27);
      expect(results[0].b).toBe(4);
      expect(results[0].c).toBe(23); 
      expect(results[0].d).toBe(16); 
    });
  });

  describe("multo (multiplication)", () => {
    it("should verify correct multiplication when all values are grounded", async () => {
      const results = await query()
        .select($ => ({
          success: true
        }))
        .where($ => multo(3, 4, 12))
        .toArray();
      
      expect(results).toHaveLength(1);
      expect(results[0].success).toBe(true);
    });

    it("should fail when multiplication is incorrect", async () => {
      const results = await query()
        .select($ => ({
          success: true
        }))
        .where($ => multo(3, 4, 13))
        .toArray();
      
      expect(results).toHaveLength(0);
    });

    it("should compute product when both factors are grounded", async () => {
      const results = await query()
        .select($ => ({
          z: $.z
        }))
        .where($ => multo(6, 7, $.z))
        .toArray();
      
      expect(results).toHaveLength(1);
      expect(results[0].z).toBe(42);
    });

    it("should compute factor when other factor and product are grounded", async () => {
      const results = await query()
        .select($ => ({
          x: $.x
        }))
        .where($ => multo($.x, 6, 42))
        .toArray();
      
      expect(results).toHaveLength(1);
      expect(results[0].x).toBe(7);
    });

    it("should handle division that doesn't result in integers", async () => {
      const results = await query()
        .select($ => ({
          x: $.x
        }))
        .where($ => multo($.x, 2, 3))
        .toArray();
      
      console.log({
        results 
      })
      expect(results[0].x).toBe(1.5); // 10/3 is not an integer
    });

    it("should handle division by zero", async () => {
      const results = await query()
        .select($ => ({
          x: $.x
        }))
        .where($ => multo($.x, 0, 5))
        .toArray();
      
      expect(results).toHaveLength(0); // Cannot divide by zero
    });
  });

  describe("dividebyo (division)", () => {
    it("should verify correct division when all values are grounded", async () => {
      const results = await query()
        .select($ => ({
          success: true
        }))
        .where($ => dividebyo(12, 4, 3))
        .toArray();
      
      expect(results).toHaveLength(1);
      expect(results[0].success).toBe(true);
    });

    it("should fail when division is incorrect", async () => {
      const results = await query()
        .select($ => ({
          success: true
        }))
        .where($ => dividebyo(13, 4, 3))
        .toArray();
      
      expect(results).toHaveLength(0);
    });

    it("should compute division when both factors are grounded", async () => {
      const results = await query()
        .select($ => ({
          z: $.z
        }))
        .where($ => dividebyo(100, 20, $.z))
        .toArray();
      
      expect(results).toHaveLength(1);
      expect(results[0].z).toBe(5);
    });

    it("should compute top num when other sumbers are grounded", async () => {
      const results = await query()
        .select($ => ({
          x: $.x
        }))
        .where($ => dividebyo($.x, 6, 7))
        .toArray();
      
      expect(results).toHaveLength(1);
      expect(results[0].x).toBe(42);
    });

    it("should handle multiplication that doesn't use integers", async () => {
      const results = await query()
        .select($ => ({
          x: $.x
        }))
        .where($ => dividebyo(3, 2, $.x))
        .toArray();
      
      console.log({
        results 
      })
      expect(results[0].x).toBe(1.5); // 10/3 is not an integer
    });

    it("should handle divide by zero", async () => {
      const results = await query()
        .select($ => ({
          x: $.x
        }))
        .where($ => dividebyo(5, 0, $.x))
        .toArray();
      
      expect(results).toHaveLength(0); // Cannot divide by zero
    });

    it("should handle top num 0", async () => {
      const results = await query()
        .select($ => ({
          x: $.x
        }))
        .where($ => dividebyo(0, 5, $.x))
        .toArray();
      
      expect(results).toHaveLength(1); // Cannot divide by zero
      expect(results[0].x).toBe(0);
    });
  });

  describe("maxo (maximum)", () => {
    it("should select substitution with maximum value", async () => {
      const input$ = new SimpleObservable<Map<string, any>>((observer) => {
        const s1 = new Map([['x', 5]]);
        const s2 = new Map([['x', 10]]);
        const s3 = new Map([['x', 3]]);
        
        observer.next(s1);
        observer.next(s2);
        observer.next(s3);
        observer.complete?.();
      });

      const x = lvar('x');
      const results: any[] = [];
      
      maxo(x)(input$).subscribe({
        next: (result) => {
          expect(result.get('x')).toBe(10);
          results.push(result);
        },
        complete: () => {
          expect(results).toHaveLength(1);
        }
      });
      
    });

    it("should handle multiple substitutions with same maximum value", async () => {
      const input$ = new SimpleObservable<Map<string, any>>((observer) => {
        const s1 = new Map([['x', 10]]);
        const s2 = new Map([['x', 5]]);
        const s3 = new Map([['x', 10]]);
        
        observer.next(s1);
        observer.next(s2);
        observer.next(s3);
        observer.complete?.();
      });

      const x = lvar('x');
      const results: any[] = [];
      
      maxo(x)(input$).subscribe({
        next: (result) => {
          expect(result.get('x')).toBe(10);
          results.push(result);
        },
        complete: () => {
          expect(results).toHaveLength(2);
        }
      });
      
    });
  });

  describe("mino (minimum)", () => {
    it("should select substitution with minimum value", async () => {
      const input$ = new SimpleObservable<Map<string, any>>((observer) => {
        const s1 = new Map([['x', 5]]);
        const s2 = new Map([['x', 10]]);
        const s3 = new Map([['x', 3]]);
        
        observer.next(s1);
        observer.next(s2);
        observer.next(s3);
        observer.complete?.();
      });

      const x = lvar('x');
      const results: any[] = [];
      
      mino(x)(input$).subscribe({
        next: (result) => {
          expect(result.get('x')).toBe(3);
          results.push(result);
        },
        complete: () => {
          expect(results).toHaveLength(1);
        }
      });
      
    });

    it("should handle multiple substitutions with same minimum value", async () => {
      const input$ = new SimpleObservable<Map<string, any>>((observer) => {
        const s1 = new Map([['x', 3]]);
        const s2 = new Map([['x', 10]]);
        const s3 = new Map([['x', 3]]);
        
        observer.next(s1);
        observer.next(s2);
        observer.next(s3);
        observer.complete?.();
      });

      const x = lvar('x');
      const results: any[] = [];
      
      mino(x)(input$).subscribe({
        next: (result) => {
          expect(result.get('x')).toBe(3);
          results.push(result);
        },
        complete: () => {
          expect(results).toHaveLength(2);
        }
      });
      
    });
  });

  describe("constraint suspension and wake-up", () => {
    it("should suspend constraint with insufficient variables and wake up later", async () => {
      const results = await query()
        .select($ => ({
          x: $.x,
          y: $.y,
          z: $.z
        }))
        .where($ => and(
          pluso($.x, $.y, $.z), // x + y = z (suspended initially)
          // Later provide values through disjunction
          or(
            and(eq($.x, 10), eq($.y, 5)), // This should wake up the constraint
            and(eq($.x, 20), eq($.y, 15))
          )
        ))
        .toArray();
      
      expect(results).toHaveLength(2);
      
      // Should have computed z from x + y in both cases
      const result1 = results.find(r => r.x === 10);
      const result2 = results.find(r => r.x === 20);
      
      expect(result1?.z).toBe(15); // 10 + 5
      expect(result2?.z).toBe(35); // 20 + 15
    });
  });
});
