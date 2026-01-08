from app.digital.concentratorIP.utils import calculate_similarity

def analyze_similar_emails(df):
    """Analiza y marca emails similares en el DataFrame"""
    df['similarity'] = 0
    df['similarity_value'] = 0.0
    df['similarity_emails'] = ''
    
    grouped = df.groupby('ip')
    
    for name, group in grouped:
        for i in range(len(group)):
            similarity_emails = []
            for j in range(len(group)):
                if i != j:
                    similarity_value = calculate_similarity(
                        group.iloc[i]['email'], 
                        group.iloc[j]['email']
                    )
                    
                    if similarity_value > 0.7:
                        df.loc[group.index[i], 'similarity'] = 1
                        similarity_emails.append(group.iloc[j]['email'])
            
            df.loc[group.index[i], 'similarity_emails'] = '|'.join(similarity_emails)
            if similarity_emails:
                max_similarity = max(
                    calculate_similarity(df.loc[group.index[i], 'email'], email) 
                    for email in similarity_emails
                )
                df.loc[group.index[i], 'similarity_value'] = max_similarity
    
    return df