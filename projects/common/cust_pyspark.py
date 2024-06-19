def add_column_byexpression(inpdf, strexp):
    cols = inpdf.columns + [strexp]
    return inpdf.selectExpr(*cols)

def gen_sqlexpr_ifelse(cond, true_mapping, false_mapping):
    return f'IF({cond}, {true_mapping}, {false_mapping})'

def gen_sqlexpr_recursive_ifelse(list__cond_and_mapping, default='"others"'):
    def gen_expr_recursively(current_list):
        if not current_list:
            return default
        else:
            (lead_cond, lead_mapping) = current_list[0]
            return gen_sqlexpr_ifelse(lead_cond, lead_mapping, gen_expr_recursively(current_list[1:]))
    return gen_expr_recursively(list__cond_and_mapping)

def print_rows(inprows):
    for irow in inprows:
        print(irow)
